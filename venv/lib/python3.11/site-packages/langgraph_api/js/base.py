import os

from langchain_core.runnables import Runnable

from langgraph_api.schema import Config

JS_EXTENSIONS = (
    ".ts",
    ".mts",
    ".cts",
    ".js",
    ".mjs",
    ".cjs",
)


def is_js_path(path: str | None) -> bool:
    if path is None:
        return False
    return os.path.splitext(path)[1] in JS_EXTENSIONS


class BaseRemotePregel(Runnable):
    name: str = "LangGraph"

    graph_id: str

    # Config passed from get_graph()
    config: Config

    async def get_nodes_executed(self) -> int:
        return 0
