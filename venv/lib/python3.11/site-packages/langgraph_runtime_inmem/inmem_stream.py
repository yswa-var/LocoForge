import asyncio
import logging
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from uuid import UUID

logger = logging.getLogger(__name__)


def _ensure_uuid(id: str | UUID) -> UUID:
    return UUID(id) if isinstance(id, str) else id


@dataclass
class Message:
    topic: bytes
    data: bytes
    id: bytes | None = None


class ContextQueue(asyncio.Queue):
    """Queue that supports async context manager protocol"""

    async def __aenter__(self):
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        # Clear the queue
        while not self.empty():
            try:
                self.get_nowait()
            except asyncio.QueueEmpty:
                break


class StreamManager:
    def __init__(self):
        self.queues = defaultdict(list)  # Dict[UUID, List[asyncio.Queue]]
        self.control_queues = defaultdict(list)

        self.message_stores = defaultdict(list)  # Dict[UUID, List[Message]]
        self.message_next_idx = defaultdict(int)  # Dict[UUID, int]

    def get_queues(self, run_id: UUID | str) -> list[asyncio.Queue]:
        run_id = _ensure_uuid(run_id)
        return self.queues[run_id]

    async def put(
        self, run_id: UUID | str, message: Message, resumable: bool = False
    ) -> None:
        run_id = _ensure_uuid(run_id)
        message.id = str(self.message_next_idx[run_id]).encode()
        self.message_next_idx[run_id] += 1
        if resumable:
            self.message_stores[run_id].append(message)
        topic = message.topic.decode()
        if "control" in topic:
            self.control_queues[run_id].append(message)
        queues = self.queues.get(run_id, [])
        coros = [queue.put(message) for queue in queues]
        results = await asyncio.gather(*coros, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.exception(f"Failed to put message in queue: {result}")

    async def add_queue(self, run_id: UUID | str) -> asyncio.Queue:
        run_id = _ensure_uuid(run_id)
        queue = ContextQueue()
        self.queues[run_id].append(queue)
        for control_msg in self.control_queues[run_id]:
            try:
                await queue.put(control_msg)
            except Exception:
                logger.exception(
                    f"Failed to put control message in queue: {control_msg}"
                )

        return queue

    async def remove_queue(self, run_id: UUID | str, queue: asyncio.Queue):
        run_id = _ensure_uuid(run_id)
        if run_id in self.queues:
            self.queues[run_id].remove(queue)
            if not self.queues[run_id]:
                del self.queues[run_id]
                if run_id in self.message_stores:
                    del self.message_stores[run_id]

    def restore_messages(
        self, run_id: UUID | str, message_id: str | None
    ) -> Iterator[Message]:
        """Get a stored message by ID for resumable streams."""
        run_id = _ensure_uuid(run_id)
        message_idx = int(message_id) + 1 if message_id else None

        if message_idx is None:
            yield from []
            return

        if run_id in self.message_stores:
            yield from self.message_stores[run_id][message_idx:]


# Global instance
stream_manager = StreamManager()


async def start_stream() -> None:
    """Initialize the queue system.
    In this in-memory implementation, we just need to ensure we have a clean StreamManager instance.
    """
    global stream_manager
    stream_manager = StreamManager()


async def stop_stream() -> None:
    """Clean up the queue system.
    Clear all queues and stored control messages."""
    global stream_manager

    # Send 'done' message to all active queues before clearing
    for run_id in list(stream_manager.queues.keys()):
        control_message = Message(topic=f"run:{run_id}:control".encode(), data=b"done")

        for queue in stream_manager.queues[run_id]:
            try:
                await queue.put(control_message)
            except (Exception, RuntimeError):
                pass  # Ignore errors during shutdown

    # Clear all stored data
    stream_manager.queues.clear()
    stream_manager.control_queues.clear()
    stream_manager.message_stores.clear()


def get_stream_manager() -> StreamManager:
    """Get the global stream manager instance."""
    return stream_manager
