import os
from os import environ, getenv
from typing import Literal, TypedDict

import orjson
from starlette.config import Config, undefined
from starlette.datastructures import CommaSeparatedStrings

# types


class CorsConfig(TypedDict, total=False):
    allow_origins: list[str]
    allow_methods: list[str]
    allow_headers: list[str]
    allow_credentials: bool
    allow_origin_regex: str
    expose_headers: list[str]
    max_age: int


class ConfigurableHeaders(TypedDict):
    includes: list[str] | None
    excludes: list[str] | None


class HttpConfig(TypedDict, total=False):
    app: str
    """Import path for a custom Starlette/FastAPI app to mount"""
    disable_assistants: bool
    """Disable /assistants routes"""
    disable_threads: bool
    """Disable /threads routes"""
    disable_runs: bool
    """Disable /runs routes"""
    disable_store: bool
    """Disable /store routes"""
    disable_meta: bool
    """Disable /ok, /info, /metrics, and /docs routes"""
    cors: CorsConfig | None
    """CORS configuration"""
    disable_ui: bool
    """Disable /ui routes"""
    disable_mcp: bool
    """Disable /mcp routes"""
    mount_prefix: str
    """Prefix for mounted routes. E.g., "/my-deployment/api"."""
    configurable_headers: ConfigurableHeaders | None


class ThreadTTLConfig(TypedDict, total=False):
    strategy: Literal["delete"]
    default_ttl: float | None
    sweep_interval_minutes: int | None


class IndexConfig(TypedDict, total=False):
    """Configuration for indexing documents for semantic search in the store."""

    dims: int
    """Number of dimensions in the embedding vectors.
    
    Common embedding models have the following dimensions:
        - OpenAI text-embedding-3-large: 256, 1024, or 3072
        - OpenAI text-embedding-3-small: 512 or 1536
        - OpenAI text-embedding-ada-002: 1536
        - Cohere embed-english-v3.0: 1024
        - Cohere embed-english-light-v3.0: 384
        - Cohere embed-multilingual-v3.0: 1024
        - Cohere embed-multilingual-light-v3.0: 384
    """

    embed: str
    """Either a path to an embedding model (./path/to/file.py:embedding_model)
    or a name of an embedding model (openai:text-embedding-3-small)
    
    Note: LangChain is required to use the model format specification.
    """

    fields: list[str] | None
    """Fields to extract text from for embedding generation.
    
    Defaults to the root ["$"], which embeds the json object as a whole.
    """


class TTLConfig(TypedDict, total=False):
    """Configuration for TTL (time-to-live) behavior in the store."""

    refresh_on_read: bool
    """Default behavior for refreshing TTLs on read operations (GET and SEARCH).
    
    If True, TTLs will be refreshed on read operations (get/search) by default.
    This can be overridden per-operation by explicitly setting refresh_ttl.
    Defaults to True if not configured.
    """
    default_ttl: float | None
    """Default TTL (time-to-live) in minutes for new items.
    
    If provided, new items will expire after this many minutes after their last access.
    The expiration timer refreshes on both read and write operations.
    Defaults to None (no expiration).
    """
    sweep_interval_minutes: int | None
    """Interval in minutes between TTL sweep operations.
    
    If provided, the store will periodically delete expired items based on TTL.
    Defaults to None (no sweeping).
    """


class StoreConfig(TypedDict, total=False):
    path: str
    index: IndexConfig
    ttl: TTLConfig


class CheckpointerConfig(TypedDict, total=False):
    """Configuration for the built-in checkpointer, which handles checkpointing of state.

    If omitted, no checkpointer is set up (the object store will still be present, however).
    """

    ttl: ThreadTTLConfig | None
    """Optional. Defines the TTL (time-to-live) behavior configuration.
    
    If provided, the checkpointer will apply TTL settings according to the configuration.
    If omitted, no TTL behavior is configured.
    """


# env

env = Config()


def _parse_json(json: str | None) -> dict | None:
    if not json:
        return None
    parsed = orjson.loads(json)
    if not parsed:
        return None
    return parsed


STATS_INTERVAL_SECS = env("STATS_INTERVAL_SECS", cast=int, default=60)

# storage

DATABASE_URI = env("DATABASE_URI", cast=str, default=getenv("POSTGRES_URI", undefined))
MIGRATIONS_PATH = env("MIGRATIONS_PATH", cast=str, default="/storage/migrations")
POSTGRES_POOL_MAX_SIZE = env("LANGGRAPH_POSTGRES_POOL_MAX_SIZE", cast=int, default=150)
RESUMABLE_STREAM_TTL_SECONDS = env(
    "RESUMABLE_STREAM_TTL_SECONDS",
    cast=int,
    default=3600,  # 1 hour
)


def _get_encryption_key(key_str: str | None):
    if not key_str:
        return None
    key = key_str.encode(encoding="utf-8")
    if len(key) not in (16, 24, 32):
        raise ValueError("LANGGRAPH_AES_KEY must be 16, 24, or 32 bytes long.")
    return key


LANGGRAPH_AES_KEY = env("LANGGRAPH_AES_KEY", default=None, cast=_get_encryption_key)

# redis
REDIS_URI = env("REDIS_URI", cast=str)
REDIS_CLUSTER = env("REDIS_CLUSTER", cast=bool, default=False)
REDIS_MAX_CONNECTIONS = env("REDIS_MAX_CONNECTIONS", cast=int, default=500)
REDIS_CONNECT_TIMEOUT = env("REDIS_CONNECT_TIMEOUT", cast=float, default=10.0)
REDIS_MAX_IDLE_TIME = env("REDIS_MAX_IDLE_TIME", cast=float, default=120.0)
REDIS_KEY_PREFIX = env("REDIS_KEY_PREFIX", cast=str, default="")

# server
ALLOW_PRIVATE_NETWORK = env("ALLOW_PRIVATE_NETWORK", cast=bool, default=False)
"""Only enable for langgraph dev when server is running on loopback address.

See https://developer.chrome.com/blog/private-network-access-update-2024-03
"""

HTTP_CONFIG: HttpConfig | None = env("LANGGRAPH_HTTP", cast=_parse_json, default=None)
STORE_CONFIG: StoreConfig | None = env(
    "LANGGRAPH_STORE", cast=_parse_json, default=None
)

MOUNT_PREFIX: str | None = env("MOUNT_PREFIX", cast=str, default=None) or (
    HTTP_CONFIG.get("mount_prefix") if HTTP_CONFIG else None
)

CORS_ALLOW_ORIGINS = env("CORS_ALLOW_ORIGINS", cast=CommaSeparatedStrings, default="*")
CORS_CONFIG: CorsConfig | None = env("CORS_CONFIG", cast=_parse_json, default=None) or (
    HTTP_CONFIG.get("cors") if HTTP_CONFIG else None
)
"""
{
    "type": "object",
    "properties": {
        "allow_origins": {
            "type": "array",
            "items": {"type": "string"},
            "default": []
        },
        "allow_methods": {
            "type": "array", 
            "items": {"type": "string"},
            "default": ["GET"]
        },
        "allow_headers": {
            "type": "array",
            "items": {"type": "string"},
            "default": []
        },
        "allow_credentials": {
            "type": "boolean",
            "default": false
        },
        "allow_origin_regex": {
            "type": ["string", "null"],
            "default": null
        },
        "expose_headers": {
            "type": "array",
            "items": {"type": "string"},
            "default": []
        },
        "max_age": {
            "type": "integer",
            "default": 600
        }
    }
}
"""
if CORS_CONFIG is not None and CORS_ALLOW_ORIGINS != "*":
    if CORS_CONFIG.get("allow_origins") is None:
        CORS_CONFIG["allow_origins"] = CORS_ALLOW_ORIGINS

# queue

BG_JOB_HEARTBEAT = 120  # seconds
BG_JOB_INTERVAL = 30  # seconds
BG_JOB_MAX_RETRIES = 3
BG_JOB_ISOLATED_LOOPS = env("BG_JOB_ISOLATED_LOOPS", cast=bool, default=False)
BG_JOB_SHUTDOWN_GRACE_PERIOD_SECS = env(
    "BG_JOB_SHUTDOWN_GRACE_PERIOD_SECS",
    cast=int,
    default=180,  # 3 minutes
)
MAX_STREAM_CHUNK_SIZE_BYTES = env(
    "MAX_STREAM_CHUNK_SIZE_BYTES", cast=int, default=1024 * 1024 * 128
)


def _parse_thread_ttl(value: str | None) -> ThreadTTLConfig | None:
    if not value:
        return None
    if str(value).strip().startswith("{"):
        return _parse_json(value.strip())
    return {
        "strategy": "delete",
        # We permit float values mainly for testing purposes
        "default_ttl": float(value),
        "sweep_interval_minutes": 5.1,
    }


CHECKPOINTER_CONFIG: CheckpointerConfig | None = env(
    "LANGGRAPH_CHECKPOINTER", cast=_parse_json, default=None
)
THREAD_TTL: ThreadTTLConfig | None = env(
    "LANGGRAPH_THREAD_TTL", cast=_parse_thread_ttl, default=None
)
if THREAD_TTL is None and CHECKPOINTER_CONFIG is not None:
    THREAD_TTL = CHECKPOINTER_CONFIG.get("ttl")

N_JOBS_PER_WORKER = env("N_JOBS_PER_WORKER", cast=int, default=10)
BG_JOB_TIMEOUT_SECS = env("BG_JOB_TIMEOUT_SECS", cast=float, default=3600)
FF_CRONS_ENABLED = env("FF_CRONS_ENABLED", cast=bool, default=True)

# auth

LANGGRAPH_AUTH_TYPE = env("LANGGRAPH_AUTH_TYPE", cast=str, default="noop")
LANGGRAPH_POSTGRES_EXTENSIONS: Literal["standard", "lite"] = env(
    "LANGGRAPH_POSTGRES_EXTENSIONS", cast=str, default="standard"
)
if LANGGRAPH_POSTGRES_EXTENSIONS not in ("standard", "lite"):
    raise ValueError(
        f"Unknown LANGGRAPH_POSTGRES_EXTENSIONS value: {LANGGRAPH_POSTGRES_EXTENSIONS}"
    )
LANGGRAPH_AUTH = env("LANGGRAPH_AUTH", cast=_parse_json, default=None)
LANGSMITH_TENANT_ID = env("LANGSMITH_TENANT_ID", cast=str, default=None)
LANGSMITH_AUTH_VERIFY_TENANT_ID = env(
    "LANGSMITH_AUTH_VERIFY_TENANT_ID",
    cast=bool,
    default=LANGSMITH_TENANT_ID is not None,
)

if LANGGRAPH_AUTH_TYPE == "langsmith":
    LANGSMITH_AUTH_ENDPOINT = env("LANGSMITH_AUTH_ENDPOINT", cast=str)
    LANGSMITH_TENANT_ID = env("LANGSMITH_TENANT_ID", cast=str)
    LANGSMITH_AUTH_VERIFY_TENANT_ID = env(
        "LANGSMITH_AUTH_VERIFY_TENANT_ID", cast=bool, default=True
    )

else:
    LANGSMITH_AUTH_ENDPOINT = env(
        "LANGSMITH_AUTH_ENDPOINT",
        cast=str,
        default=getenv(
            "LANGCHAIN_ENDPOINT",
            getenv("LANGSMITH_ENDPOINT", "https://api.smith.langchain.com"),
        ),
    )

# license

LANGGRAPH_CLOUD_LICENSE_KEY = env("LANGGRAPH_CLOUD_LICENSE_KEY", cast=str, default="")
LANGSMITH_API_KEY = env(
    "LANGSMITH_API_KEY", cast=str, default=getenv("LANGCHAIN_API_KEY", "")
)

# if langsmith api key is set, enable tracing unless explicitly disabled

if (
    LANGSMITH_API_KEY
    and not getenv("LANGCHAIN_TRACING_V2")
    and not getenv("LANGCHAIN_TRACING")
    and not getenv("LANGSMITH_TRACING_V2")
    and not getenv("LANGSMITH_TRACING")
):
    environ["LANGCHAIN_TRACING_V2"] = "true"

TRACING = (
    env("LANGCHAIN_TRACING_V2", cast=bool, default=None)
    or env("LANGCHAIN_TRACING", cast=bool, default=None)
    or env("LANGSMITH_TRACING_V2", cast=bool, default=None)
    or env("LANGSMITH_TRACING", cast=bool, default=None)
)

# if variant is "licensed", update to "local" if using LANGSMITH_API_KEY instead

if getenv("LANGSMITH_LANGGRAPH_API_VARIANT") == "licensed" and LANGSMITH_API_KEY:
    environ["LANGSMITH_LANGGRAPH_API_VARIANT"] = "local"


# Metrics.
USES_INDEXING = (
    STORE_CONFIG
    and STORE_CONFIG.get("index")
    and STORE_CONFIG.get("index").get("embed")
)
USES_CUSTOM_APP = HTTP_CONFIG and HTTP_CONFIG.get("app")
USES_CUSTOM_AUTH = bool(LANGGRAPH_AUTH)
USES_THREAD_TTL = bool(THREAD_TTL)
USES_STORE_TTL = bool(STORE_CONFIG and STORE_CONFIG.get("ttl"))

API_VARIANT = env("LANGSMITH_LANGGRAPH_API_VARIANT", cast=str, default="")

# UI
UI_USE_BUNDLER = env("LANGGRAPH_UI_BUNDLER", cast=bool, default=False)
IS_QUEUE_ENTRYPOINT = False

if not os.getenv("LANGCHAIN_REVISION_ID") and (
    ref_sha := os.getenv("LANGSMITH_LANGGRAPH_GIT_REF_SHA")
):
    # This is respected by the langsmith SDK env inference
    # https://github.com/langchain-ai/langsmith-sdk/blob/1b93e4c13b8369d92db891ae3babc3e2254f0e56/python/langsmith/env/_runtime_env.py#L190
    os.environ["LANGCHAIN_REVISION_ID"] = ref_sha
