import asyncio
import signal
from contextlib import asynccontextmanager
from typing import Any

import structlog
from langchain_core.runnables.config import var_child_runnable_config
from langgraph.constants import CONF, CONFIG_KEY_STORE
from starlette.applications import Starlette

from langgraph_runtime_inmem import queue
from langgraph_runtime_inmem.database import start_pool, stop_pool

logger = structlog.stdlib.get_logger(__name__)


@asynccontextmanager
async def lifespan(
    app: Starlette | None = None,
    taskset: set[asyncio.Task] | None = None,
    **kwargs: Any,
):
    import langgraph_api.config as config
    from langgraph_api import __version__, graph, thread_ttl
    from langgraph_api import store as api_store
    from langgraph_api.asyncio import SimpleTaskGroup, set_event_loop
    from langgraph_api.http import start_http_client, stop_http_client
    from langgraph_api.js.ui import start_ui_bundler, stop_ui_bundler
    from langgraph_api.metadata import metadata_loop

    from langgraph_runtime_inmem import __version__ as langgraph_runtime_inmem_version

    await logger.ainfo(
        f"Starting In-Memory runtime with langgraph-api={__version__} and in-memory runtime={langgraph_runtime_inmem_version}",
        version=__version__,
        langgraph_runtime_inmem_version=langgraph_runtime_inmem_version,
    )
    try:
        current_loop = asyncio.get_running_loop()
        set_event_loop(current_loop)
    except RuntimeError:
        await logger.aerror("Failed to set loop")

    await start_http_client()
    await start_pool()
    await start_ui_bundler()
    try:
        async with SimpleTaskGroup(
            cancel=True,
            taskset=taskset,
            taskgroup_name="Lifespan",
        ) as tg:
            tg.create_task(metadata_loop())
            await api_store.collect_store_from_env()
            store_instance = await api_store.get_store()
            if not api_store.CUSTOM_STORE:
                tg.create_task(store_instance.start_ttl_sweeper())  # type: ignore
            else:
                await logger.ainfo("Using custom store. Skipping store TTL sweeper.")
            tg.create_task(thread_ttl.thread_ttl_sweep_loop())
            var_child_runnable_config.set({CONF: {CONFIG_KEY_STORE: store_instance}})

            # Keep after the setter above so users can access the store from within the factory function
            await graph.collect_graphs_from_env(True)
            if config.N_JOBS_PER_WORKER > 0:
                tg.create_task(queue_with_signal())

            yield
    finally:
        await api_store.exit_store()
        await stop_ui_bundler()
        await graph.stop_remote_graphs()
        await stop_http_client()
        await stop_pool()


async def queue_with_signal():
    try:
        await queue.queue()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.exception("Queue failed. Signaling shutdown", exc_info=exc)
        signal.raise_signal(signal.SIGINT)
