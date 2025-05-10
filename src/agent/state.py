"""Define the state structures for the agent."""

from __future__ import annotations
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class State:
    """Defines the state for the multi-agent system.
    
    This class maintains the state of the conversation and agent interactions.
    """
    messages: List[Dict[str, Any]] = field(default_factory=list)
    current_agent: Optional[str] = None
    task_type: Optional[str] = None
    sql_result: Optional[Dict[str, Any]] = None
    nosql_result: Optional[Dict[str, Any]] = None
    drive_result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status: Optional[str] = None  # Added to track operation status
