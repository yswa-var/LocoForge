"""
Graph workflow nodes for the Hybrid Orchestrator
"""

from typing import Dict, Any
from my_agent.utils.state import OrchestratorState, QueryDomain
from my_agent.utils.orchestrator_agent import HybridOrchestrator
import json
import os
import logging
from dotenv import load_dotenv

# Set up logging for LangGraph Studio
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables explicitly for LangGraph Studio
load_dotenv()

# Initialize orchestrator with lazy loading
_orchestrator_instance = None

def get_orchestrator():
    """Get or create orchestrator instance with proper error handling"""
    global _orchestrator_instance
    
    if _orchestrator_instance is None:
        try:
            logger.info("🔄 Initializing orchestrator for LangGraph Studio...")
            
            # Force reload environment variables
            load_dotenv()
            
            # Check environment before initialization
            mongo_db = "mongodb://localhost:27017/"
            openai_key = os.getenv("OPENAI_API_KEY")
            sql_db = os.getenv("SQL_DB")
            
            logger.info(f"LangGraph Studio Environment Check:")
            logger.info(f"  MONGO_DB: {'SET' if mongo_db else 'NOT SET'}")
            logger.info(f"  OPENAI_API_KEY: {'SET' if openai_key else 'NOT SET'}")
            logger.info(f"  SQL_DB: {'SET' if sql_db else 'NOT SET'}")
            
            _orchestrator_instance = HybridOrchestrator()
            
            # Check if agents are properly initialized
            status = _orchestrator_instance.check_agent_status()
            
            if not status['nosql_agent']['initialized']:
                logger.warning(f"⚠️  NoSQL agent not initialized: {status['nosql_agent'].get('error', 'Unknown error')}")
                logger.warning(f"   Available: {status['nosql_agent']['available']}")
                logger.warning(f"   Environment: {status['environment']}")
            else:
                logger.info("✅ NoSQL agent initialized successfully")
                
            if not status['sql_agent']['initialized']:
                logger.warning(f"⚠️  SQL agent not initialized: {status['sql_agent'].get('error', 'Unknown error')}")
                logger.warning(f"   Available: {status['sql_agent']['available']}")
            else:
                logger.info("✅ SQL agent initialized successfully")
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize orchestrator: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Create a minimal orchestrator instance for error handling
            _orchestrator_instance = HybridOrchestrator()
    
    return _orchestrator_instance

def reset_orchestrator():
    """Force re-initialization of the orchestrator (useful for LangGraph Studio)"""
    global _orchestrator_instance
    _orchestrator_instance = None
    logger.info("🔄 Orchestrator reset - will re-initialize on next use")
    return get_orchestrator()

def check_orchestrator_status():
    """Check the status of the orchestrator and its agents"""
    try:
        orchestrator = get_orchestrator()
        return orchestrator.check_agent_status()
    except Exception as e:
        logger.error(f"Failed to check orchestrator status: {e}")
        return {
            "error": f"Failed to check orchestrator status: {e}",
            "orchestrator_available": False
        }

def initialize_state(state: OrchestratorState) -> OrchestratorState:
    """Initialize missing state fields with defaults"""
    # Get the user query from the last message
    if state.get("messages") and len(state["messages"]) > 0:
        last_message = state["messages"][-1]
        if hasattr(last_message, 'content'):
            state["current_query"] = last_message.content
        else:
            state["current_query"] = str(last_message)
    else:
        state["current_query"] = ""
    
    # Initialize other fields if missing
    if "query_domain" not in state:
        state["query_domain"] = None
    if "query_intent" not in state:
        state["query_intent"] = None
    if "sub_queries" not in state:
        state["sub_queries"] = {}
    if "sql_results" not in state:
        state["sql_results"] = None
    if "nosql_results" not in state:
        state["nosql_results"] = None
    if "combined_results" not in state:
        state["combined_results"] = None
    if "context_history" not in state:
        state["context_history"] = []
    if "execution_path" not in state:
        state["execution_path"] = []
    if "error_message" not in state:
        state["error_message"] = None
    
    return state

def classify_query_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Classify query domain and intent"""
    # Initialize state if needed
    state = initialize_state(state)
    
    query = state["current_query"]
    
    if not query:
        state["error_message"] = "No query provided"
        return state
    
    # Classify the query
    domain, intent = get_orchestrator().classify_intent(query)
    
    # Update state
    state["query_domain"] = domain
    state["query_intent"] = intent
    state["execution_path"].append("classify_query")
    
    return state

def decompose_query_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Decompose complex queries into sub-queries"""
    # Initialize state if needed
    state = initialize_state(state)
    
    query = state["current_query"]
    domain = state["query_domain"]
    
    if not query or not domain:
        state["error_message"] = "Missing query or domain classification"
        return state
    
    # Decompose query
    sub_queries = get_orchestrator().decompose_query(query, domain)
    state["sub_queries"] = sub_queries
    state["execution_path"].append("decompose_query")
    
    return state

def route_to_agents_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Route to appropriate agent(s) based on domain"""
    # Initialize state if needed
    state = initialize_state(state)
    
    domain = state["query_domain"]
    sub_queries = state["sub_queries"]
    
    if not domain:
        state["error_message"] = "No domain classification available"
        return state
    
    if domain == QueryDomain.EMPLOYEE:
        # Execute SQL only
        sql_query = sub_queries.get("employee", state["current_query"])
        logger.info(f"[DEBUG] SQL Query sent to agent: {sql_query}")
        sql_result = get_orchestrator().execute_sql_query(sql_query)
        logger.info(f"[DEBUG] SQL Agent result: {sql_result}")
        state["sql_results"] = sql_result
        state["execution_path"].append("sql_agent")
        
    elif domain == QueryDomain.WAREHOUSE:
        # Execute NoSQL only
        nosql_query = sub_queries.get("warehouse", state["current_query"])
        # Ensure query is a string
        if isinstance(nosql_query, dict) and 'content' in nosql_query:
            nosql_query = nosql_query['content']
        logger.info(f"[DEBUG] NoSQL Query sent to agent: {nosql_query}")
        state["nosql_results"] = get_orchestrator().execute_nosql_query(nosql_query)
        logger.info(f"[DEBUG] NoSQL Agent result: {state['nosql_results']}")
        state["execution_path"].append("nosql_agent")
        
    elif domain == QueryDomain.HYBRID:
        # Execute both agents
        sql_query = sub_queries.get("sql", "")
        nosql_query = sub_queries.get("nosql", "")
        
        if sql_query:
            logger.info(f"[DEBUG] SQL Query sent to agent: {sql_query}")
            state["sql_results"] = get_orchestrator().execute_sql_query(sql_query)
            logger.info(f"[DEBUG] SQL Agent result: {state['sql_results']}")
        if nosql_query:
            # Ensure query is a string
            if isinstance(nosql_query, dict) and 'content' in nosql_query:
                nosql_query = nosql_query['content']
            logger.info(f"[DEBUG] NoSQL Query sent to agent: {nosql_query}")
            state["nosql_results"] = get_orchestrator().execute_nosql_query(nosql_query)
            logger.info(f"[DEBUG] NoSQL Agent result: {state['nosql_results']}")
        
        state["execution_path"].append("both_agents")
    
    return state

def aggregate_results_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Aggregate results from multiple agents"""
    # Initialize state if needed
    state = initialize_state(state)
    
    domain = state["query_domain"]
    
    if domain == QueryDomain.HYBRID:
        # Combine results from both agents
        sql_results = state.get("sql_results", {"success": False, "data": []})
        nosql_results = state.get("nosql_results", {"success": False, "data": []})
        
        combined_results = get_orchestrator().aggregate_results(
            sql_results, nosql_results, state["current_query"]
        )
        state["combined_results"] = combined_results
        
    elif domain == QueryDomain.EMPLOYEE:
        # Format SQL results directly
        sql_results = state.get("sql_results", {})
        sql_exec = sql_results.get("execution_result", {})
        if sql_results:
            state["combined_results"] = {
                "success": sql_exec.get("success", False),
                "original_query": state["current_query"],
                "timestamp": get_orchestrator()._get_timestamp(),
                "data_sources": ["sql"],
                "sql_data": {
                    "success": sql_exec.get("success", False),
                    "query": sql_results.get("generated_sql", "N/A"),
                    "row_count": sql_exec.get("row_count", 0),
                    "data": sql_exec.get("data", [])
                } if sql_exec.get("success", False) else {
                    "success": False,
                    "error": sql_exec.get("error", "Unknown error")
                }
            }
        else:
            state["combined_results"] = {
                "success": False,
                "error": "No SQL results available",
                "original_query": state["current_query"]
            }
        
    elif domain == QueryDomain.WAREHOUSE:
        # Format NoSQL results directly
        nosql_results = state.get("nosql_results", {})
        if nosql_results:
            execution_result = nosql_results.get("execution_result", {})
            state["combined_results"] = {
                "success": execution_result.get("success", False),
                "original_query": state["current_query"],
                "timestamp": get_orchestrator()._get_timestamp(),
                "data_sources": ["nosql"],
                "nosql_data": {
                    "success": execution_result.get("success", False),
                    "query": nosql_results.get("generated_mongodb_query", "N/A"),
                    "row_count": execution_result.get("row_count", 0),
                    "data": execution_result.get("data", [])
                } if execution_result.get("success", False) else {
                    "success": False,
                    "error": execution_result.get("error", "Unknown error")
                }
            }
        else:
            state["combined_results"] = {
                "success": False,
                "error": "No NoSQL results available",
                "original_query": state["current_query"]
            }
    
    state["execution_path"].append("aggregate_results")
    return state

def update_context_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Update conversation context"""
    # Initialize state if needed
    state = initialize_state(state)
    
    state = get_orchestrator().update_context(state)
    state["execution_path"].append("update_context")
    return state

def format_response_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Format final response for user"""
    # Initialize state if needed
    state = initialize_state(state)
    
    combined_results = state.get("combined_results", {})
    
    # Handle None case
    if combined_results is None:
        combined_results = {}
    
    if combined_results.get("success", False):
        # Return successful results as JSON
        response_text = json.dumps(combined_results, indent=2, default=str)
    else:
        # Format error response
        error_msg = combined_results.get("error", "Unknown error occurred")
        error_response = {
            "success": False,
            "error": error_msg,
            "original_query": state.get("current_query", ""),
            "timestamp": get_orchestrator()._get_timestamp()
        }
        response_text = json.dumps(error_response, indent=2, default=str)
        state["error_message"] = error_msg
    
    # Add AI response to messages
    from langchain_core.messages import AIMessage
    state["messages"].append(AIMessage(content=response_text))
    
    state["execution_path"].append("format_response")
    return state

def route_decision(state: OrchestratorState) -> str:
    """Decision function for routing based on domain"""
    # Initialize state if needed
    state = initialize_state(state)
    
    domain = state["query_domain"]
    
    if domain == QueryDomain.EMPLOYEE:
        return "sql_only"
    elif domain == QueryDomain.WAREHOUSE:
        return "nosql_only"
    elif domain == QueryDomain.HYBRID:
        return "both_agents"
    else:
        return "error_handling" 