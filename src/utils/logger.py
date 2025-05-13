"""Logging configuration for the Query Orchestrator."""

import sys
import asyncio
from pathlib import Path
from loguru import logger
from typing import Any, Dict, Optional
import threading
import weakref

from .config import settings

# Thread-local storage for async components
_thread_local = threading.local()

# Keep track of active queues to prevent memory leaks
_active_queues = weakref.WeakSet()

def get_log_queue() -> Optional[asyncio.Queue]:
    """Get or create the log queue for the current event loop."""
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        return None

    if not hasattr(_thread_local, 'log_queue') or not hasattr(_thread_local, 'loop') or _thread_local.loop != current_loop:
        # Create new queue for this event loop
        _thread_local.log_queue = asyncio.Queue()
        _thread_local.loop = current_loop
        _active_queues.add(_thread_local.log_queue)
    
    return _thread_local.log_queue

async def async_sink(message: str) -> None:
    """Async sink for loguru that writes to a queue."""
    try:
        queue = get_log_queue()
        if queue is not None:
            await queue.put(message)
    except Exception:
        # If queue operation fails, write directly to stderr
        sys.stderr.write(message)
        sys.stderr.flush()

async def process_logs() -> None:
    """Process logs from the queue."""
    queue = get_log_queue()
    if queue is None:
        return

    try:
        while True:
            try:
                message = await queue.get()
                try:
                    sys.stderr.write(message)
                    sys.stderr.flush()
                except Exception as e:
                    print(f"Error writing log: {e}", file=sys.stderr)
                finally:
                    queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error in log processing: {e}", file=sys.stderr)
                # Break the loop on error to prevent infinite error messages
                break
    finally:
        # Clean up the queue
        if queue in _active_queues:
            _active_queues.remove(queue)
        if hasattr(_thread_local, 'log_queue'):
            del _thread_local.log_queue
        if hasattr(_thread_local, 'loop'):
            del _thread_local.loop

def setup_logging() -> None:
    """Configure logging for the application."""
    # Remove default logger
    logger.remove()
    
    # Add console logger with async sink
    logger.add(
        async_sink,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=settings.log_level,
        colorize=True,
    )
    
    # Add file logger with synchronous sink
    log_path = Path("logs")
    log_path.mkdir(exist_ok=True)
    
    logger.add(
        str(log_path / "query_orchestrator_{time}.log"),
        rotation="1 day",
        retention="7 days",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=settings.log_level,
        enqueue=True,  # Use queue for thread safety
    )

async def initialize_async_logging() -> None:
    """Initialize async logging components."""
    # Create a new queue for this event loop
    queue = get_log_queue()
    if queue is not None:
        # Start the log processing task
        task = asyncio.create_task(process_logs())
        # Store task reference to prevent garbage collection
        _thread_local.log_task = task

async def cleanup_async_logging() -> None:
    """Clean up async logging components."""
    if hasattr(_thread_local, 'log_task'):
        task = _thread_local.log_task
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        del _thread_local.log_task
    
    if hasattr(_thread_local, 'log_queue'):
        queue = _thread_local.log_queue
        if queue in _active_queues:
            _active_queues.remove(queue)
        del _thread_local.log_queue
    
    if hasattr(_thread_local, 'loop'):
        del _thread_local.loop

# Configure basic logging on module import
setup_logging()

# Export logger and async initialization function
__all__ = ["logger", "initialize_async_logging", "cleanup_async_logging"] 