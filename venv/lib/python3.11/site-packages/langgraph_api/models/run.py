import asyncio
import functools
import re
import time
import urllib.parse
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, NamedTuple, TypedDict
from uuid import UUID

import orjson
from langgraph.checkpoint.base.id import uuid6
from starlette.authentication import BaseUser
from starlette.exceptions import HTTPException

from langgraph_api.graph import GRAPHS, get_assistant_id
from langgraph_api.schema import (
    All,
    Config,
    IfNotExists,
    MetadataInput,
    MultitaskStrategy,
    OnCompletion,
    Run,
    RunCommand,
    StreamMode,
)
from langgraph_api.utils import AsyncConnectionProto, get_auth_ctx
from langgraph_runtime.ops import Runs, logger


class LangSmithTracer(TypedDict, total=False):
    """Configuration for LangSmith tracing."""

    example_id: str | None
    project_name: str | None


class RunCreateDict(TypedDict):
    """Payload for creating a run."""

    assistant_id: str
    """Assistant ID to use for this run."""
    checkpoint_id: str | None
    """Checkpoint ID to start from. Defaults to the latest checkpoint."""
    input: Sequence[dict] | dict[str, Any] | None
    """Input to the run. Pass null to resume from the current state of the thread."""
    command: RunCommand | None
    """One or more commands to update the graph's state and send messages to nodes."""
    metadata: MetadataInput
    """Metadata for the run."""
    config: Config | None
    """Additional configuration for the run."""
    webhook: str | None
    """Webhook to call when the run is complete."""

    interrupt_before: All | list[str] | None
    """Interrupt execution before entering these nodes."""
    interrupt_after: All | list[str] | None
    """Interrupt execution after leaving these nodes."""

    multitask_strategy: MultitaskStrategy
    """Strategy to handle concurrent runs on the same thread. Only relevant if
    there is a pending/inflight run on the same thread. One of:
    - "reject": Reject the new run.
    - "interrupt": Interrupt the current run, keeping steps completed until now,
       and start a new one.
    - "rollback": Cancel and delete the existing run, rolling back the thread to
      the state before it had started, then start the new run.
    - "enqueue": Queue up the new run to start after the current run finishes.
    """
    on_completion: OnCompletion
    """What to do when the run completes. One of:
    - "keep": Keep the thread in the database.
    - "delete": Delete the thread from the database.
    """
    stream_mode: list[StreamMode] | StreamMode
    """One or more of "values", "messages", "updates" or "events".
    - "values": Stream the thread state any time it changes.
    - "messages": Stream chat messages from thread state and calls to chat models, 
      token-by-token where possible.
    - "updates": Stream the state updates returned by each node.
    - "events": Stream all events produced by sub-runs (eg. nodes, LLMs, etc.).
    - "custom": Stream custom events produced by your nodes.
    """
    stream_subgraphs: bool | None
    """Stream output from subgraphs. By default, streams only the top graph."""
    stream_resumable: bool | None
    """Whether to persist the stream chunks in order to resume the stream later."""
    feedback_keys: list[str] | None
    """Pass one or more feedback_keys if you want to request short-lived signed URLs
    for submitting feedback to LangSmith with this key for this run."""
    after_seconds: int | None
    """Start the run after this many seconds. Defaults to 0."""
    if_not_exists: IfNotExists
    """Create the thread if it doesn't exist. If False, reply with 404."""
    langsmith_tracer: LangSmithTracer | None
    """Configuration for additional tracing with LangSmith."""


def ensure_ids(
    assistant_id: str | UUID,
    thread_id: str | UUID | None,
    payload: RunCreateDict,
) -> tuple[uuid.UUID, uuid.UUID | None, uuid.UUID | None]:
    try:
        results = [
            assistant_id if isinstance(assistant_id, UUID) else UUID(assistant_id)
        ]
    except ValueError:
        keys = ", ".join(GRAPHS.keys())
        raise HTTPException(
            status_code=422,
            detail=f"Invalid assistant: '{assistant_id}'. Must be either:\n"
            f"- A valid assistant UUID, or\n"
            f"- One of the registered graphs: {keys}",
        ) from None
    if thread_id:
        try:
            results.append(
                thread_id if isinstance(thread_id, UUID) else UUID(thread_id)
            )
        except ValueError:
            raise HTTPException(status_code=422, detail="Invalid thread ID") from None
    else:
        results.append(None)
    if checkpoint_id := payload.get("checkpoint_id"):
        try:
            results.append(
                checkpoint_id
                if isinstance(checkpoint_id, UUID)
                else UUID(checkpoint_id)
            )
        except ValueError:
            raise HTTPException(
                status_code=422, detail="Invalid checkpoint ID"
            ) from None
    else:
        results.append(None)
    return tuple(results)


def assign_defaults(
    payload: RunCreateDict,
):
    if payload.get("stream_mode"):
        stream_mode = (
            payload["stream_mode"]
            if isinstance(payload["stream_mode"], list)
            else [payload["stream_mode"]]
        )
    else:
        stream_mode = ["values"]
    multitask_strategy = payload.get("multitask_strategy") or "reject"
    prevent_insert_if_inflight = multitask_strategy == "reject"
    return stream_mode, multitask_strategy, prevent_insert_if_inflight


def get_user_id(user: BaseUser | None) -> str | None:
    if user is None:
        return None
    try:
        return user.identity
    except NotImplementedError:
        try:
            return user.display_name
        except NotImplementedError:
            pass


LANGSMITH_METADATA = "langsmith-metadata"
LANGSMITH_TAGS = "langsmith-tags"
LANGSMITH_PROJECT = "langsmith-project"


def translate_pattern(pat: str) -> re.Pattern[str]:
    """Translate a pattern to regex, supporting only literals and * wildcards to avoid RE DoS."""
    res = []
    i = 0
    n = len(pat)

    while i < n:
        c = pat[i]
        i += 1

        if c == "*":
            res.append(".*")
        else:
            res.append(re.escape(c))

    pattern = "".join(res)
    return re.compile(rf"(?s:{pattern})\Z")


@functools.lru_cache(maxsize=1)
def get_header_patterns() -> tuple[
    list[re.Pattern[str] | None], list[re.Pattern[str] | None]
]:
    from langgraph_api import config

    if not config.HTTP_CONFIG:
        return None, None
    configurable = config.HTTP_CONFIG.get("configurable_headers")
    if not configurable:
        return None, None
    header_includes = configurable.get("includes") or []
    include_patterns = []
    for include in header_includes:
        include_patterns.append(translate_pattern(include))
    header_excludes = configurable.get("excludes") or []
    exclude_patterns = []
    for exclude in header_excludes:
        exclude_patterns.append(translate_pattern(exclude))
    return include_patterns, exclude_patterns


def get_configurable_headers(headers: dict[str, str]) -> dict[str, str]:
    configurable = {}
    include_patterns, exclude_patterns = get_header_patterns()
    for key, value in headers.items():
        # First handle tracing stuff; not configurable
        if key == "langsmith-trace":
            configurable[key] = value
            if baggage := headers.get("baggage"):
                for item in baggage.split(","):
                    key, value = item.split("=")
                    if key == LANGSMITH_METADATA and key not in configurable:
                        configurable[key] = orjson.loads(urllib.parse.unquote(value))
                    elif key == LANGSMITH_TAGS:
                        configurable[key] = urllib.parse.unquote(value).split(",")
                    elif key == LANGSMITH_PROJECT:
                        configurable[key] = urllib.parse.unquote(value)
        # Then handle overridable behavior
        if exclude_patterns and any(pattern.match(key) for pattern in exclude_patterns):
            continue
        if include_patterns and any(pattern.match(key) for pattern in include_patterns):
            configurable[key] = value
            continue

        # Then handle default behavior
        if key.startswith("x-"):
            if key in (
                "x-api-key",
                "x-tenant-id",
                "x-service-key",
            ):
                continue
            configurable[key] = value

        elif key == "user-agent":
            configurable[key] = value
    return configurable


async def create_valid_run(
    conn: AsyncConnectionProto,
    thread_id: str | None,
    payload: RunCreateDict,
    headers: Mapping[str, str],
    barrier: asyncio.Barrier | None = None,
    run_id: UUID | None = None,
    request_start_time: float | None = None,
) -> Run:
    request_id = headers.get("x-request-id")  # Will be null in the crons scheduler.
    (
        assistant_id,
        thread_id,
        checkpoint_id,
        run_id,
    ) = _get_ids(
        thread_id,
        payload,
        run_id=run_id,
    )
    if (
        thread_id is None
        and (command := payload.get("command"))
        and command.get("resume")
    ):
        raise HTTPException(
            status_code=400,
            detail="You must provide a thread_id when resuming.",
        )
    temporary = thread_id is None and payload.get("on_completion", "delete") == "delete"
    stream_mode, multitask_strategy, prevent_insert_if_inflight = assign_defaults(
        payload
    )
    # assign custom headers and checkpoint to config
    config = payload.get("config") or {}
    configurable = config.setdefault("configurable", {})
    if checkpoint_id:
        configurable["checkpoint_id"] = str(checkpoint_id)
    if checkpoint := payload.get("checkpoint"):
        configurable.update(checkpoint)
    configurable.update(get_configurable_headers(headers))
    ctx = get_auth_ctx()
    if ctx:
        user = ctx.user
        user_id = get_user_id(user)
        configurable["langgraph_auth_user"] = user
        configurable["langgraph_auth_user_id"] = user_id
        configurable["langgraph_auth_permissions"] = ctx.permissions
    else:
        user_id = None
    if not configurable.get("langgraph_request_id"):
        configurable["langgraph_request_id"] = request_id
    if ls_tracing := payload.get("langsmith_tracer"):
        configurable["__langsmith_project__"] = ls_tracing.get("project_name")
        configurable["__langsmith_example_id__"] = ls_tracing.get("example_id")
    if request_start_time:
        configurable["__request_start_time_ms__"] = request_start_time
    after_seconds = payload.get("after_seconds", 0)
    configurable["__after_seconds__"] = after_seconds
    put_time_start = time.time()
    run_coro = Runs.put(
        conn,
        assistant_id,
        {
            "input": payload.get("input"),
            "command": payload.get("command"),
            "config": config,
            "stream_mode": stream_mode,
            "interrupt_before": payload.get("interrupt_before"),
            "interrupt_after": payload.get("interrupt_after"),
            "webhook": payload.get("webhook"),
            "feedback_keys": payload.get("feedback_keys"),
            "temporary": temporary,
            "subgraphs": payload.get("stream_subgraphs", False),
            "resumable": payload.get("stream_resumable", False),
            "checkpoint_during": payload.get("checkpoint_during", True),
        },
        metadata=payload.get("metadata"),
        status="pending",
        user_id=user_id,
        thread_id=thread_id,
        run_id=run_id,
        multitask_strategy=multitask_strategy,
        prevent_insert_if_inflight=prevent_insert_if_inflight,
        after_seconds=after_seconds,
        if_not_exists=payload.get("if_not_exists", "reject"),
    )
    run_ = await run_coro

    if barrier:
        await barrier.wait()

    # abort if thread, assistant, etc not found
    try:
        first = await anext(run_)
    except StopAsyncIteration:
        raise HTTPException(
            status_code=404, detail="Thread or assistant not found."
        ) from None

    # handle multitask strategy
    inflight_runs = [run async for run in run_]
    if first["run_id"] == run_id:
        logger.info(
            "Created run",
            run_id=str(run_id),
            thread_id=str(thread_id),
            assistant_id=str(assistant_id),
            multitask_strategy=multitask_strategy,
            stream_mode=stream_mode,
            temporary=temporary,
            after_seconds=after_seconds,
            if_not_exists=payload.get("if_not_exists", "reject"),
            run_create_ms=(
                int(time.time() * 1_000) - request_start_time
                if request_start_time
                else None
            ),
            run_put_ms=int((time.time() - put_time_start) * 1_000),
        )
        # inserted, proceed
        if multitask_strategy in ("interrupt", "rollback") and inflight_runs:
            try:
                await Runs.cancel(
                    conn,
                    [run["run_id"] for run in inflight_runs],
                    thread_id=thread_id,
                    action=multitask_strategy,
                )
            except HTTPException:
                # if we can't find the inflight runs again, we can proceeed
                pass
        return first
    elif multitask_strategy == "reject":
        raise HTTPException(
            status_code=409,
            detail="Thread is already running a task. Wait for it to finish or choose a different multitask strategy.",
        )
    else:
        raise NotImplementedError


class _Ids(NamedTuple):
    assistant_id: uuid.UUID
    thread_id: uuid.UUID | None
    checkpoint_id: uuid.UUID | None
    run_id: uuid.UUID


def _get_ids(
    thread_id: str | None,
    payload: RunCreateDict,
    run_id: UUID | None = None,
) -> _Ids:
    # get assistant_id
    assistant_id = get_assistant_id(payload["assistant_id"])

    # ensure UUID validity defaults
    assistant_id, thread_id, checkpoint_id = ensure_ids(
        assistant_id, thread_id, payload
    )

    run_id = run_id or uuid6()

    return _Ids(
        assistant_id,
        thread_id,
        checkpoint_id,
        run_id,
    )
