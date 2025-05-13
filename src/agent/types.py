"""Shared types for the Query Orchestrator."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TypedDict
from enum import Enum
from pydantic import BaseModel


class DatabaseType(str, Enum):
    """Supported database types."""
    SQL = "sql"
    NOSQL = "nosql"
    GOOGLE_DRIVE = "google_drive"


class TaskStatus(str, Enum):
    """Task status enumeration."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class Task(BaseModel):
    """Task definition for database operations."""
    task_id: str
    database_type: DatabaseType
    query: str
    priority: int
    dependencies: List[str] = field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class Configuration(BaseModel):
    """Configuration for the orchestrator."""
    model_name: str = "gpt-3.5-turbo"
    temperature: float = 0.7
    max_retries: int = 3
    enable_parallel_execution: bool = True


@dataclass
class State:
    """System state for the orchestrator."""
    user_query: Optional[str] = None
    tasks: List[Task] = field(default_factory=list)
    current_task: Optional[Task] = None
    results: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)
    messages: List[Dict[str, str]] = field(default_factory=list)

    def __post_init__(self):
        """Post-initialization processing."""
        if self.user_query is None and self.messages:
            # Extract user query from messages if available
            if isinstance(self.messages, list) and self.messages:
                last_message = self.messages[-1]
                if isinstance(last_message, dict) and "content" in last_message:
                    self.user_query = last_message["content"]
                elif isinstance(last_message, str):
                    self.user_query = last_message 