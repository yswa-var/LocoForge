import contextlib
import json
import logging
import os
import pathlib
import threading
import typing
from collections.abc import Mapping, Sequence
from typing import Literal

from typing_extensions import TypedDict

if typing.TYPE_CHECKING:
    from langgraph_api.config import HttpConfig, StoreConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_ls_origin() -> str | None:
    from langsmith.client import Client
    from langsmith.utils import tracing_is_enabled

    if not tracing_is_enabled():
        return
    client = Client()
    return client._host_url


def _get_org_id() -> str | None:
    from langsmith.client import Client
    from langsmith.utils import tracing_is_enabled

    # Yes, the organizationId is actually the workspace iD
    # which is actually the tenantID which we actually get via
    # the sessions endpoint
    if not tracing_is_enabled():
        return
    client = Client()
    try:
        response = client.request_with_retries(
            "GET", "/api/v1/sessions", params={"limit": 1}
        )
        result = response.json()
        if result:
            return result[0]["tenant_id"]
    except Exception as e:
        logger.debug("Failed to get organization ID: %s", str(e))
        return None


@contextlib.contextmanager
def patch_environment(**kwargs):
    """Temporarily patch environment variables.

    Args:
        **kwargs: Key-value pairs of environment variables to set.

    Yields:
        None
    """
    original = {}
    try:
        for key, value in kwargs.items():
            if value is None:
                original[key] = os.environ.pop(key, None)
                continue
            original[key] = os.environ.get(key)
            os.environ[key] = value
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class SecurityConfig(TypedDict, total=False):
    securitySchemes: dict
    security: list
    # path => {method => security}
    paths: dict[str, dict[str, list]]


class AuthConfig(TypedDict, total=False):
    path: str
    """Path to the authentication function in a Python file."""
    disable_studio_auth: bool
    """Whether to disable auth when connecting from the LangSmith Studio."""
    openapi: SecurityConfig
    """The schema to use for updating the openapi spec.

    Example:
        {
            "securitySchemes": {
                "OAuth2": {
                    "type": "oauth2",
                    "flows": {
                        "password": {
                            "tokenUrl": "/token",
                            "scopes": {
                                "me": "Read information about the current user",
                                "items": "Access to create and manage items"
                            }
                        }
                    }
                }
            },
            "security": [
                {"OAuth2": ["me"]}  # Default security requirement for all endpoints
            ]
        }
    """


def _check_newer_version(pkg: str, timeout: float = 0.2) -> None:
    """Log a notice if PyPI reports a newer version."""
    import importlib.metadata as md
    import json
    import urllib.request

    from packaging.version import Version

    thread_logger = logging.getLogger("check_version")
    if not thread_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        thread_logger.addHandler(handler)

    try:
        current = Version(md.version(pkg))
        with urllib.request.urlopen(
            f"https://pypi.org/pypi/{pkg}/json", timeout=timeout
        ) as resp:
            latest_str = json.load(resp)["info"]["version"]
        latest = Version(latest_str)
        if latest > current:
            thread_logger.info(
                "🔔  A newer version of %s is available: %s → %s (pip install -U %s)",
                pkg,
                current,
                latest,
                pkg,
            )

    except Exception:
        pass

    except RuntimeError:
        thread_logger.info(
            f"Failed to check for newer version of {pkg}."
            " To disable version checks, set LANGGRAPH_NO_VERSION_CHECK=true"
        )


def run_server(
    host: str = "127.0.0.1",
    port: int = 2024,
    reload: bool = False,
    graphs: dict | None = None,
    n_jobs_per_worker: int | None = None,
    env_file: str | None = None,
    open_browser: bool = False,
    tunnel: bool = False,
    debug_port: int | None = None,
    wait_for_client: bool = False,
    env: str | pathlib.Path | Mapping[str, str] | None = None,
    reload_includes: Sequence[str] | None = None,
    reload_excludes: Sequence[str] | None = None,
    store: typing.Optional["StoreConfig"] = None,
    auth: AuthConfig | None = None,
    http: typing.Optional["HttpConfig"] = None,
    ui: dict | None = None,
    ui_config: dict | None = None,
    studio_url: str | None = None,
    disable_persistence: bool = False,
    allow_blocking: bool = False,
    runtime_edition: Literal["inmem", "community", "postgres"] = "inmem",
    server_level: str = "WARNING",
    **kwargs: typing.Any,
):
    """Run the LangGraph API server."""

    import inspect
    import time

    import uvicorn

    start_time = time.time()

    env_vars = env if isinstance(env, Mapping) else None
    mount_prefix = None
    if http is not None and http.get("mount_prefix") is not None:
        mount_prefix = http.get("mount_prefix")
    if os.environ.get("LANGGRAPH_MOUNT_PREFIX"):
        mount_prefix = os.environ.get("LANGGRAPH_MOUNT_PREFIX")
    if isinstance(env, str | pathlib.Path):
        try:
            from dotenv.main import DotEnv

            env_vars = DotEnv(dotenv_path=env).dict() or {}
            logger.debug(f"Loaded environment variables from {env}: {sorted(env_vars)}")

        except ImportError:
            logger.warning(
                "python_dotenv is not installed. Environment variables will not be available."
            )

    if debug_port is not None:
        try:
            import debugpy
        except ImportError:
            logger.warning("debugpy is not installed. Debugging will not be available.")
            logger.info("To enable debugging, install debugpy: pip install debugpy")
            return
        debugpy.listen((host, debug_port))
        logger.info(
            f"🐛 Debugger listening on port {debug_port}. Waiting for client to attach..."
        )
        logger.info("To attach the debugger:")
        logger.info("1. Open your python debugger client (e.g., Visual Studio Code).")
        logger.info(
            "2. Use the 'Remote Attach' configuration with the following settings:"
        )
        logger.info("   - Host: 0.0.0.0")
        logger.info(f"   - Port: {debug_port}")
        logger.info("3. Start the debugger to connect to the server.")
        if wait_for_client:
            debugpy.wait_for_client()
            logger.info("Debugger attached. Starting server...")

    # Determine local or tunneled URL
    upstream_url = f"http://{host}:{port}"
    if mount_prefix:
        upstream_url += mount_prefix
    if tunnel:
        logger.info("Starting Cloudflare Tunnel...")
        from concurrent.futures import TimeoutError as FutureTimeoutError

        from langgraph_api.tunneling.cloudflare import start_tunnel

        tunnel_obj = start_tunnel(port)
        try:
            public_url = tunnel_obj.url.result(timeout=30)
        except FutureTimeoutError:
            logger.warning(
                "Timed out waiting for Cloudflare Tunnel URL; using local URL %s",
                upstream_url,
            )
            public_url = upstream_url
        except Exception as e:
            tunnel_obj.process.kill()
            raise RuntimeError("Failed to start Cloudflare Tunnel") from e
        local_url = public_url
        if mount_prefix:
            local_url += mount_prefix
    else:
        local_url = upstream_url
    to_patch = dict(
        MIGRATIONS_PATH="__inmem",
        DATABASE_URI=":memory:",
        REDIS_URI="fake",
        N_JOBS_PER_WORKER=str(n_jobs_per_worker if n_jobs_per_worker else 1),
        LANGGRAPH_STORE=json.dumps(store) if store else None,
        LANGSERVE_GRAPHS=json.dumps(graphs) if graphs else None,
        LANGSMITH_LANGGRAPH_API_VARIANT="local_dev",
        LANGGRAPH_AUTH=json.dumps(auth) if auth else None,
        LANGGRAPH_HTTP=json.dumps(http) if http else None,
        LANGGRAPH_UI=json.dumps(ui) if ui else None,
        LANGGRAPH_UI_CONFIG=json.dumps(ui_config) if ui_config else None,
        LANGGRAPH_UI_BUNDLER="true",
        LANGGRAPH_API_URL=local_url,
        LANGGRAPH_DISABLE_FILE_PERSISTENCE=str(disable_persistence).lower(),
        LANGGRAPH_RUNTIME_EDITION=runtime_edition,
        # If true, we will not raise on blocking IO calls (via blockbuster)
        LANGGRAPH_ALLOW_BLOCKING=str(allow_blocking).lower(),
        # See https://developer.chrome.com/blog/private-network-access-update-2024-03
        ALLOW_PRIVATE_NETWORK="true",
    )
    if env_vars is not None:
        # Don't overwrite.
        for k, v in env_vars.items():
            if k in to_patch:
                logger.debug(f"Skipping loaded env var {k}={v}")
                continue
            to_patch[k] = v
    with patch_environment(
        **to_patch,
    ):
        studio_origin = studio_url or _get_ls_origin() or "https://smith.langchain.com"
        full_studio_url = f"{studio_origin}/studio/?baseUrl={local_url}"

        def _open_browser():
            nonlocal studio_origin, full_studio_url
            import time
            import urllib.request
            import webbrowser
            from concurrent.futures import ThreadPoolExecutor

            thread_logger = logging.getLogger("browser_opener")
            if not thread_logger.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter("%(message)s"))
                thread_logger.addHandler(handler)

            with ThreadPoolExecutor(max_workers=1) as executor:
                org_id_future = executor.submit(_get_org_id)

                while True:
                    try:
                        with urllib.request.urlopen(f"{local_url}/ok") as response:
                            if response.status == 200:
                                try:
                                    org_id = org_id_future.result(timeout=3.0)
                                    if org_id:
                                        full_studio_url = f"{studio_origin}/studio/?baseUrl={local_url}&organizationId={org_id}"
                                except TimeoutError as e:
                                    thread_logger.debug(
                                        f"Failed to get organization ID: {str(e)}"
                                    )
                                    pass
                                thread_logger.info(
                                    f"Server started in {time.time() - start_time:.2f}s"
                                )
                                thread_logger.info(
                                    "🎨 Opening Studio in your browser..."
                                )
                                thread_logger.info("URL: " + full_studio_url)
                                webbrowser.open(full_studio_url)
                                return
                    except urllib.error.URLError:
                        pass
                    time.sleep(0.1)

        welcome = f"""

        Welcome to

╦  ┌─┐┌┐┌┌─┐╔═╗┬─┐┌─┐┌─┐┬ ┬
║  ├─┤││││ ┬║ ╦├┬┘├─┤├─┘├─┤
╩═╝┴ ┴┘└┘└─┘╚═╝┴└─┴ ┴┴  ┴ ┴

- 🚀 API: \033[36m{local_url}\033[0m
- 🎨 Studio UI: \033[36m{full_studio_url}\033[0m
- 📚 API Docs: \033[36m{local_url}/docs\033[0m

This in-memory server is designed for development and testing.
For production use, please use LangGraph Cloud.

"""
        logger.info(welcome)
        if open_browser:
            threading.Thread(target=_open_browser, daemon=True).start()
        nvc = os.getenv("LANGGRAPH_NO_VERSION_CHECK")
        if nvc is None or nvc.lower() not in ("true", "1"):
            print("Checking for newer version...")
            threading.Thread(
                target=_check_newer_version, args=("langgraph-api",), daemon=True
            ).start()
        supported_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k in inspect.signature(uvicorn.run).parameters
        }
        server_level = server_level.upper()
        uvicorn.run(
            "langgraph_api.server:app",
            host=host,
            port=port,
            reload=reload,
            env_file=env_file,
            access_log=False,
            reload_includes=reload_includes,
            reload_excludes=reload_excludes,
            log_config={
                "version": 1,
                "incremental": False,
                "disable_existing_loggers": False,
                "formatters": {
                    "simple": {
                        "class": "langgraph_api.logging.Formatter",
                    }
                },
                "handlers": {
                    "console": {
                        "class": "logging.StreamHandler",
                        "formatter": "simple",
                        "stream": "ext://sys.stdout",
                    }
                },
                "loggers": {
                    "uvicorn": {"level": server_level},
                    "uvicorn.error": {"level": server_level},
                    "langgraph_api.server": {"level": server_level},
                },
                "root": {"handlers": ["console"]},
            },
            **supported_kwargs,
        )


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="CLI entrypoint for running the LangGraph API server."
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="Host to bind the server to"
    )
    parser.add_argument(
        "--port", type=int, default=2024, help="Port to bind the server to"
    )
    parser.add_argument("--no-reload", action="store_true", help="Disable auto-reload")
    parser.add_argument(
        "--config", default="langgraph.json", help="Path to configuration file"
    )
    parser.add_argument(
        "--n-jobs-per-worker",
        type=int,
        help="Number of jobs per worker. Default is None (meaning 10)",
    )
    parser.add_argument(
        "--no-browser", action="store_true", help="Disable automatic browser opening"
    )
    parser.add_argument(
        "--debug-port", type=int, help="Port for debugger to listen on (default: none)"
    )
    parser.add_argument(
        "--wait-for-client",
        action="store_true",
        help="Whether to break and wait for a debugger to attach",
    )
    parser.add_argument(
        "--tunnel",
        action="store_true",
        help="Expose the server via Cloudflare Tunnel",
    )

    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        config_data = json.load(f)

    graphs = config_data.get("graphs", {})
    auth = config_data.get("auth")
    ui = config_data.get("ui")
    ui_config = config_data.get("ui_config")
    run_server(
        args.host,
        args.port,
        not args.no_reload,
        graphs,
        n_jobs_per_worker=args.n_jobs_per_worker,
        open_browser=not args.no_browser,
        tunnel=args.tunnel,
        debug_port=args.debug_port,
        wait_for_client=args.wait_for_client,
        env=config_data.get("env", None),
        auth=auth,
        ui=ui,
        ui_config=ui_config,
    )


if __name__ == "__main__":
    main()
