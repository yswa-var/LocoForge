from typing import List, TypedDict, Dict, Any, Optional
from langchain_core.messages import BaseMessage
from enum import Enum

class QueryDomain(Enum):
    EMPLOYEE = "employee"
    WAREHOUSE = "warehouse"
    HYBRID = "hybrid"
    UNKNOWN = "unknown"

class QueryIntent(Enum):
    SELECT = "select"
    ANALYZE = "analyze"
    COMPARE = "compare"
    AGGREGATE = "aggregate"

class OrchestratorState(TypedDict):
    """Enhanced state for the orchestrator workflow."""
    messages: List[BaseMessage]
    current_query: str
    query_domain: QueryDomain
    query_intent: QueryIntent
    sub_queries: Dict[str, str]
    sql_results: Optional[Dict[str, Any]]
    nosql_results: Optional[Dict[str, Any]]
    combined_results: Optional[Dict[str, Any]]
    context_history: List[Dict[str, Any]]
    execution_path: List[str]
    error_message: Optional[str]

class ChatState(TypedDict):
    """State for the chat application."""
    messages: List[BaseMessage] 