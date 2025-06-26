import asyncio
import os
import uuid
from datetime import UTC, datetime

import langgraph.version
import orjson
import structlog

from langgraph_api.config import (
    LANGGRAPH_CLOUD_LICENSE_KEY,
    LANGSMITH_API_KEY,
    LANGSMITH_AUTH_ENDPOINT,
    USES_CUSTOM_APP,
    USES_CUSTOM_AUTH,
    USES_INDEXING,
    USES_STORE_TTL,
    USES_THREAD_TTL,
)
from langgraph_api.http import http_request
from langgraph_license.validation import plus_features_enabled

logger = structlog.stdlib.get_logger(__name__)

INTERVAL = 300
REVISION = os.getenv("LANGSMITH_LANGGRAPH_API_REVISION")
VARIANT = os.getenv("LANGSMITH_LANGGRAPH_API_VARIANT")
PROJECT_ID = os.getenv("LANGSMITH_HOST_PROJECT_ID")
TENANT_ID = os.getenv("LANGSMITH_TENANT_ID")
if PROJECT_ID:
    try:
        uuid.UUID(PROJECT_ID)
    except ValueError:
        raise ValueError(
            f"Invalid project ID: {PROJECT_ID}. Must be a valid UUID"
        ) from None
if TENANT_ID:
    try:
        uuid.UUID(TENANT_ID)
    except ValueError:
        raise ValueError(
            f"Invalid tenant ID: {TENANT_ID}. Must be a valid UUID"
        ) from None
if VARIANT == "cloud":
    HOST = "saas"
elif PROJECT_ID:
    HOST = "byoc"
else:
    HOST = "self-hosted"
PLAN = "enterprise" if plus_features_enabled() else "developer"
USER_API_URL = os.getenv("LANGGRAPH_API_URL", None)

RUN_COUNTER = 0
NODE_COUNTER = 0
FROM_TIMESTAMP = datetime.now(UTC).isoformat()

if (
    "api.smith.langchain.com" in LANGSMITH_AUTH_ENDPOINT
    and not LANGGRAPH_CLOUD_LICENSE_KEY
):
    METADATA_ENDPOINT = LANGSMITH_AUTH_ENDPOINT.rstrip("/") + "/v1/metadata/submit"
else:
    METADATA_ENDPOINT = "https://api.smith.langchain.com/v1/metadata/submit"


def incr_runs(*, incr: int = 1) -> None:
    global RUN_COUNTER
    RUN_COUNTER += incr


def incr_nodes(_, *, incr: int = 1) -> None:
    global NODE_COUNTER
    NODE_COUNTER += incr


async def metadata_loop() -> None:
    try:
        from langgraph_api import __version__
    except ImportError:
        __version__ = None
    if not LANGGRAPH_CLOUD_LICENSE_KEY and not LANGSMITH_API_KEY:
        return

    if LANGGRAPH_CLOUD_LICENSE_KEY and not LANGGRAPH_CLOUD_LICENSE_KEY.startswith(
        "lcl_"
    ):
        logger.info("Running in air-gapped mode, skipping metadata loop")
        return

    logger.info("Starting metadata loop")

    global RUN_COUNTER, NODE_COUNTER, FROM_TIMESTAMP
    while True:
        # because we always read and write from coroutines in main thread
        # we don't need a lock as long as there's no awaits in this block
        from_timestamp = FROM_TIMESTAMP
        to_timestamp = datetime.now(UTC).isoformat()
        nodes = NODE_COUNTER
        runs = RUN_COUNTER
        RUN_COUNTER = 0
        NODE_COUNTER = 0
        FROM_TIMESTAMP = to_timestamp

        payload = {
            "license_key": LANGGRAPH_CLOUD_LICENSE_KEY,
            "api_key": LANGSMITH_API_KEY,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "tags": {
                # Tag values must be strings.
                "langgraph.python.version": langgraph.version.__version__,
                "langgraph_api.version": __version__ or "",
                "langgraph.platform.revision": REVISION or "",
                "langgraph.platform.variant": VARIANT or "",
                "langgraph.platform.host": HOST,
                "langgraph.platform.tenant_id": TENANT_ID or "",
                "langgraph.platform.project_id": PROJECT_ID or "",
                "langgraph.platform.plan": PLAN,
                # user app features
                "user_app.uses_indexing": str(USES_INDEXING or ""),
                "user_app.uses_custom_app": str(USES_CUSTOM_APP or ""),
                "user_app.uses_custom_auth": str(USES_CUSTOM_AUTH),
                "user_app.uses_thread_ttl": str(USES_THREAD_TTL),
                "user_app.uses_store_ttl": str(USES_STORE_TTL),
            },
            "measures": {
                "langgraph.platform.runs": runs,
                "langgraph.platform.nodes": nodes,
            },
            "logs": [],
        }
        try:
            await http_request(
                "POST",
                METADATA_ENDPOINT,
                body=orjson.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
        except Exception as e:
            # retry on next iteration
            incr_runs(incr=runs)
            incr_nodes("", incr=nodes)
            FROM_TIMESTAMP = from_timestamp
            await logger.ainfo("Metadata submission skipped.", error=str(e))
        await asyncio.sleep(INTERVAL)
