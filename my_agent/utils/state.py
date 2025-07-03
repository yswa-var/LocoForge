from typing import List, TypedDict, Dict, Any, Optional, Annotated
from langchain_core.messages import BaseMessage
from enum import Enum

class QueryDomain(Enum):
    EMPLOYEE = "employee"
    MOVIES = "movies"
    HYBRID = "hybrid"
    UNKNOWN = "unknown"
    UNCLEAR = "unclear"  
    TECHNICAL = "technical"  

class QueryIntent(Enum):
    SELECT = "select"
    ANALYZE = "analyze"
    COMPARE = "compare"
    AGGREGATE = "aggregate"
    CLARIFY = "clarify"  
    EXPLAIN = "explain"  

class QueryComplexity(Enum):
    SIMPLE = "simple"
    MEDIUM = "medium"
    COMPLEX = "complex"

class OrchestratorState(TypedDict):
    """Enhanced state for the orchestrator workflow."""
    messages: Annotated[List[BaseMessage], "add"]
    current_query: str
    query_domain: Optional[QueryDomain]
    query_intent: Optional[QueryIntent]
    query_complexity: Optional[QueryComplexity]  # New field for complexity assessment
    sub_queries: Dict[str, str]
    sql_results: Optional[Dict[str, Any]]
    nosql_results: Optional[Dict[str, Any]]
    combined_results: Optional[Dict[str, Any]]
    context_history: List[Dict[str, Any]]
    execution_path: List[str]
    error_message: Optional[str]
    clarification_suggestions: Optional[List[str]]  # New field for query refinement
    data_engineer_response: Optional[str]  # New field for data engineer agent responses

class ChatState(TypedDict):
    """State for the chat application."""
    messages: Annotated[List[BaseMessage], "add"] 