"""
Graph workflow nodes for the Hybrid Orchestrator
"""

from typing import Dict, Any
from my_agent.utils.state import OrchestratorState, QueryDomain, QueryIntent, QueryComplexity
from my_agent.utils.orchestrator_agent import HybridOrchestrator
from my_agent.utils.data_engineer_agent import DataEngineerAgent
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
_data_engineer_instance = None

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
            openai_key = os.getenv("OPENAPI_KEY")
            sql_db = os.getenv("SQL_DB")
            
            logger.info(f"LangGraph Studio Environment Check:")
            logger.info(f"  MONGO_DB: {'SET' if mongo_db else 'NOT SET'}")
            logger.info(f"  OPENAPI_KEY: {'SET' if openai_key else 'NOT SET'}")
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

def get_data_engineer():
    """Get or create data engineer agent instance"""
    global _data_engineer_instance
    
    if _data_engineer_instance is None:
        try:
            logger.info("🔄 Initializing Data Engineer Agent...")
            _data_engineer_instance = DataEngineerAgent()
            logger.info("✅ Data Engineer Agent initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Data Engineer Agent: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Create a minimal instance for error handling
            _data_engineer_instance = DataEngineerAgent()
    
    return _data_engineer_instance

def reset_orchestrator():
    """Force re-initialization of the orchestrator (useful for LangGraph Studio)"""
    global _orchestrator_instance, _data_engineer_instance
    _orchestrator_instance = None
    _data_engineer_instance = None
    logger.info("🔄 Orchestrator and Data Engineer reset - will re-initialize on next use")
    return get_orchestrator()

def check_orchestrator_status():
    """Check the status of the orchestrator and its agents"""
    try:
        orchestrator = get_orchestrator()
        data_engineer = get_data_engineer()
        return {
            **orchestrator.check_agent_status(),
            "data_engineer_available": data_engineer is not None
        }
    except Exception as e:
        logger.error(f"Failed to check orchestrator status: {e}")
        return {
            "error": f"Failed to check orchestrator status: {e}",
            "orchestrator_available": False,
            "data_engineer_available": False
        }

def initialize_state(state: OrchestratorState) -> OrchestratorState:
    """Initialize missing state fields with defaults"""
    # Get the user query from the last message
    if state.get("messages") and len(state["messages"]) > 0:
        last_message = state["messages"][-1]
        # Always extract string from dict if needed
        if hasattr(last_message, 'content'):
            content = last_message.content
        elif isinstance(last_message, dict) and 'content' in last_message:
            content = last_message['content']
        else:
            content = str(last_message)
        # If content is a dict, extract again
        if isinstance(content, dict) and 'content' in content:
            state["current_query"] = content['content']
        else:
            state["current_query"] = content
    else:
        # If current_query is already set but might be a string representation of a dict
        current_query = state.get("current_query", "")
        if current_query and current_query.startswith("{") and current_query.endswith("}"):
            try:
                import ast
                # Try to safely evaluate the string representation
                query_dict = ast.literal_eval(current_query)
                if isinstance(query_dict, dict) and 'content' in query_dict:
                    state["current_query"] = query_dict['content']
                else:
                    state["current_query"] = str(query_dict)
            except:
                # If parsing fails, keep as is
                pass
        elif not current_query:
            state["current_query"] = ""
    
    # Initialize other fields if missing
    if "query_domain" not in state:
        state["query_domain"] = None
    if "query_intent" not in state:
        state["query_intent"] = None
    if "query_complexity" not in state:
        state["query_complexity"] = None
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
    if "clarification_suggestions" not in state:
        state["clarification_suggestions"] = None
    if "data_engineer_response" not in state:
        state["data_engineer_response"] = None
    
    return state

def classify_query_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Classify query domain and intent with enhanced edge case handling"""
    # Initialize state if needed
    state = initialize_state(state)
    
    query = state["current_query"]
    # Always extract string from dict if needed
    if isinstance(query, dict) and 'content' in query:
        query = query['content']
        state["current_query"] = query
    
    if not query:
        state["error_message"] = "No query provided"
        return state
    
    # First, check if this is a direct SQL or NoSQL query
    if is_direct_sql_query(query):
        # Route directly to SQL agent
        state["query_domain"] = QueryDomain.EMPLOYEE
        state["query_intent"] = QueryIntent.SELECT
        state["query_complexity"] = QueryComplexity.MEDIUM
        state["execution_path"].append("classify_query")
        return state
    
    if is_direct_nosql_query(query):
        # Route directly to NoSQL agent
        state["query_domain"] = QueryDomain.MOVIES
        state["query_intent"] = QueryIntent.SELECT
        state["query_complexity"] = QueryComplexity.MEDIUM
        state["execution_path"].append("classify_query")
        return state
    
    # For non-direct queries, analyze with Data Engineer Agent
    data_engineer = get_data_engineer()
    analysis = data_engineer.analyze_query(query)
    
    # Update state with analysis results
    state["query_complexity"] = QueryComplexity(analysis.get("complexity_level", "simple"))
    
    # Handle different query types based on analysis
    query_type = analysis.get("query_type", "clear")
    
    if query_type in ["ambiguous", "non_domain", "technical"]:
        # Route to Data Engineer Agent for handling
        state["query_domain"] = QueryDomain.UNCLEAR
        state["query_intent"] = QueryIntent.CLARIFY
        
        # Handle specific query types with Data Engineer Agent
        if query_type == "ambiguous":
            response = data_engineer.handle_ambiguous_query(query, analysis)
            state["data_engineer_response"] = response
            # Also generate clarification suggestions for additional help
            suggestions = data_engineer.provide_clarification_suggestions(query, analysis)
            state["clarification_suggestions"] = suggestions
        elif query_type == "technical":
            response = data_engineer.handle_technical_query(query)
            state["data_engineer_response"] = response
        elif query_type == "non_domain":
            response = data_engineer.handle_non_domain_query(query)
            state["data_engineer_response"] = response
        
        state["execution_path"].append("classify_query")
        return state
    
    # For clear queries, proceed with normal classification
    domain, intent = get_orchestrator().classify_intent(query)
    
    # Update state
    state["query_domain"] = domain
    state["query_intent"] = intent
    state["execution_path"].append("classify_query")
    
    return state

def is_direct_sql_query(query: str) -> bool:
    """Check if the query is a direct SQL command"""
    # Clean the query and convert to uppercase for pattern matching
    clean_query = query.strip().upper()
    
    # Common SQL keywords that indicate a direct SQL query
    sql_keywords = [
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
        'SHOW', 'DESCRIBE', 'EXPLAIN', 'USE', 'GRANT', 'REVOKE'
    ]
    
    # Check if query starts with SQL keywords
    for keyword in sql_keywords:
        if clean_query.startswith(keyword):
            return True
    
    # Check for common SQL patterns, but be more specific to avoid false positives
    # Only consider it SQL if it has both a SELECT and FROM pattern
    has_select = clean_query.startswith('SELECT')
    has_from = any(pattern in clean_query for pattern in [' FROM ', 'FROM '])
    
    if has_select and has_from:
        return True
    
    # For other SQL commands, check if they start with SQL keywords
    for keyword in sql_keywords:
        if clean_query.startswith(keyword):
            return True
    
    # Additional check: if it contains SQL patterns but is natural language, don't classify as SQL
    # Look for natural language indicators
    natural_language_indicators = [
        'SHOW ME', 'TELL ME', 'FIND', 'LIST', 'GET', 'DISPLAY', 'PRINT',
        'WHAT', 'HOW', 'WHEN', 'WHERE', 'WHY', 'WHO', 'WHICH'
    ]
    
    # If it starts with natural language indicators, it's not a direct SQL query
    for indicator in natural_language_indicators:
        if clean_query.startswith(indicator):
            return False
    
    return False

def is_direct_nosql_query(query: str) -> bool:
    """Check if the query is a direct NoSQL/MongoDB command"""
    # Clean the query and convert to lowercase for pattern matching
    clean_query = query.strip().lower()
    
    # MongoDB/NoSQL patterns
    nosql_patterns = [
        'db.', 'collection.', 'find(', 'findone(', 'aggregate(',
        'insert(', 'update(', 'delete(', 'remove(',
        '{$', '{$match', '{$group', '{$sort', '{$project',
        '{$lookup', '{$unwind', '{$limit', '{$skip'
    ]
    
    for pattern in nosql_patterns:
        if pattern in clean_query:
            return True
    
    # Check for JSON-like structures that might be MongoDB queries
    if ('{' in clean_query and '}' in clean_query) and any(op in clean_query for op in ['$', 'find', 'aggregate']):
        return True
    
    return False

def decompose_query_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Decompose complex queries into sub-queries"""
    # Initialize state if needed
    state = initialize_state(state)
    
    query = state["current_query"]
    domain = state["query_domain"]
    
    if not query or not domain:
        state["error_message"] = "Missing query or domain classification"
        return state
    
    # Skip decomposition for unclear queries (handled by Data Engineer)
    if domain == QueryDomain.UNCLEAR:
        state["sub_queries"] = {"unclear": query}
        state["execution_path"].append("decompose_query")
        return state
    
    # For direct SQL queries, pass through without decomposition
    if is_direct_sql_query(query):
        state["sub_queries"] = {"employee": query}
        state["execution_path"].append("decompose_query")
        return state
    
    # For direct NoSQL queries, pass through without decomposition
    if is_direct_nosql_query(query):
        state["sub_queries"] = {"movies": query}
        state["execution_path"].append("decompose_query")
        return state
    
    # Debug logging
    logger.info(f"[DEBUG] Decomposing query: '{query}' for domain: {domain}")
    
    # Decompose query
    sub_queries = get_orchestrator().decompose_query(query, domain)
    
    # Debug logging
    logger.info(f"[DEBUG] Decomposition result: {sub_queries}")
    
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
    
    # This node now just prepares the routing decision
    # The actual agent execution will be handled by separate nodes
    state["execution_path"].append("route_to_agents")
    
    return state

def data_engineer_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Handle unclear, irrelevant, and technical queries"""
    # Initialize state if needed
    state = initialize_state(state)
    
    query = state["current_query"]
    data_engineer_response = state.get("data_engineer_response")
    clarification_suggestions = state.get("clarification_suggestions")
    
    # Use the Data Engineer Agent's response if available
    if data_engineer_response and data_engineer_response.get("success"):
        response_content = data_engineer_response["response"]
    else:
        # Fallback response with database context
        response_content = """I understand your query is unclear. Let me help you understand what data is available in our system:

**Available Databases:**

**SQL Database (Employee Management System):**
- employees: employee information, salaries, departments
- departments: department details and budgets  
- projects: project assignments and status
- attendance: employee attendance records

**NoSQL Database (Sample Mflix - Movies):**
- movies: movie information, ratings, cast, directors, genres
- comments: user comments on movies
- users: user information and accounts
- sessions: user session data
- theaters: movie theater locations
- embedded_movies: movies with vector embeddings for search

You can ask specific questions like:
- "Show all employees in the IT department"
- "Find action movies with high ratings"
- "Find employees with salary above $50,000"
- "Show movies from 2020"

What specific information would you like to see?"""
    
    # Add clarification suggestions if available and not already included in response
    if clarification_suggestions and "clarification_suggestions" not in response_content.lower():
        response_content += "\n\nHere are some specific suggestions to help clarify your query:\n"
        for i, suggestion in enumerate(clarification_suggestions, 1):
            response_content += f"{i}. {suggestion}\n"
    
    # Create combined results for unclear queries
    state["combined_results"] = {
        "success": True,
        "original_query": query,
        "query_type": "unclear",
        "response": response_content,
        "clarification_suggestions": clarification_suggestions,
        "timestamp": get_orchestrator()._get_timestamp()
    }
    
    state["execution_path"].append("data_engineer")
    return state

def sql_agent_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Execute SQL queries using SQL agent"""
    # Initialize state if needed
    state = initialize_state(state)
    
    sub_queries = state["sub_queries"]
    domain = state["query_domain"]
    
    # Determine SQL query based on domain
    if domain == QueryDomain.EMPLOYEE:
        sql_query = sub_queries.get("employee", state["current_query"])
    elif domain == QueryDomain.HYBRID:
        sql_query = sub_queries.get("sql", "")
    else:
        sql_query = ""
    
    logger.info(f"[DEBUG] SQL Agent Node - Domain: {domain}")
    logger.info(f"[DEBUG] SQL Agent Node - Sub queries: {sub_queries}")
    logger.info(f"[DEBUG] SQL Agent Node - Current query: {state['current_query']}")
    logger.info(f"[DEBUG] SQL Agent Node - SQL query to execute: {sql_query}")
    
    if sql_query:
        try:
            logger.info(f"[DEBUG] SQL Query sent to agent: {sql_query}")
            sql_result = get_orchestrator().execute_sql_query(sql_query)
            logger.info(f"[DEBUG] SQL Agent result: {sql_result}")
            state["sql_results"] = sql_result
        except Exception as e:
            logger.error(f"[ERROR] SQL Agent execution failed: {e}")
            import traceback
            logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")
            state["sql_results"] = {
                "success": False, 
                "error": f"SQL execution failed: {str(e)}",
                "execution_result": {
                    "success": False,
                    "error": str(e)
                }
            }
    else:
        logger.warning(f"[WARNING] No SQL query provided for domain: {domain}")
        state["sql_results"] = {"success": False, "error": "No SQL query provided"}
    
    state["execution_path"].append("sql_agent")
    return state

def nosql_agent_node(state: OrchestratorState) -> OrchestratorState:
    """Node: Execute NoSQL queries using NoSQL agent"""
    # Initialize state if needed
    state = initialize_state(state)
    
    sub_queries = state["sub_queries"]
    domain = state["query_domain"]
    
    # Determine NoSQL query based on domain
    if domain == QueryDomain.MOVIES:
        nosql_query = sub_queries.get("movies", state["current_query"])
    elif domain == QueryDomain.HYBRID:
        nosql_query = sub_queries.get("nosql", "")
    else:
        nosql_query = ""
    
    if nosql_query:
        # Ensure query is a string
        if isinstance(nosql_query, dict) and 'content' in nosql_query:
            nosql_query = nosql_query['content']
        logger.info(f"[DEBUG] NoSQL Query sent to agent: {nosql_query}")
        state["nosql_results"] = get_orchestrator().execute_nosql_query(nosql_query)
        logger.info(f"[DEBUG] NoSQL Agent result: {state['nosql_results']}")
    else:
        state["nosql_results"] = {"success": False, "error": "No NoSQL query provided"}
    
    state["execution_path"].append("nosql_agent")
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
        
    elif domain == QueryDomain.MOVIES:
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
    
    elif domain == QueryDomain.UNCLEAR:
        # For unclear queries, the combined_results should already be set by data_engineer_node
        # Just ensure it exists
        if "combined_results" not in state or state["combined_results"] is None:
            state["combined_results"] = {
                "success": False,
                "error": "Data Engineer Agent response not available",
                "original_query": state["current_query"],
                "timestamp": get_orchestrator()._get_timestamp()
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
    """Node: Format final response for user in professional markdown format"""
    # Initialize state if needed
    state = initialize_state(state)
    
    combined_results = state.get("combined_results", {})
    
    # Handle None case
    if combined_results is None:
        combined_results = {}
    
    if combined_results.get("success", False):
        # Format successful results in professional markdown
        response_text = format_success_response_markdown(combined_results, state.get("current_query", ""))
    else:
        # Format error response in markdown
        error_msg = combined_results.get("error", "Unknown error occurred")
        response_text = format_error_response_markdown(error_msg, state.get("current_query", ""))
        state["error_message"] = error_msg
    
    # Add AI response to messages using proper LangGraph pattern
    from langchain_core.messages import AIMessage
    # Create a new message to add to the state
    ai_message = AIMessage(content=response_text)
    
    # Return the message to be added to the messages list
    # LangGraph will handle the addition through the Annotated type
    state["messages"] = [ai_message]
    
    state["execution_path"].append("format_response")
    return state

def format_success_response_markdown(results: dict, original_query: str) -> str:
    """Format successful results in professional markdown"""
    
    # Start with header
    markdown = f"# Query Results\n\n"
    markdown += f"**Original Query:** {original_query}\n\n"
    
    # Add timestamp if available
    if "timestamp" in results:
        markdown += f"**Timestamp:** {results['timestamp']}\n\n"
    
    # Handle different data sources
    if "data_sources" in results:
        markdown += f"**Data Sources:** {', '.join(results['data_sources'])}\n\n"
    
    # Handle SQL data
    if "sql_data" in results:
        sql_data = results["sql_data"]
        if sql_data.get("success", False):
            markdown += "## SQL Database Results\n\n"
            
            # Add SQL query in code block
            if "query" in sql_data and sql_data["query"] != "N/A":
                markdown += "**Executed SQL Query:**\n"
                markdown += f"```sql\n{sql_data['query']}\n```\n\n"
            
            # Add row count
            if "row_count" in sql_data:
                markdown += f"**Rows Returned:** {sql_data['row_count']}\n\n"
            
            # Add data in JSON code block
            if "data" in sql_data and sql_data["data"]:
                markdown += "**Results:**\n"
                markdown += f"```json\n{json.dumps(sql_data['data'], indent=2, default=str)}\n```\n\n"
            else:
                markdown += "**Results:** No data returned\n\n"
        else:
            markdown += "## SQL Database Results\n\n"
            markdown += f"❌ **Error:** {sql_data.get('error', 'Unknown SQL error')}\n\n"
    
    # Handle NoSQL data
    if "nosql_data" in results:
        nosql_data = results["nosql_data"]
        if nosql_data.get("success", False):
            markdown += "## NoSQL Database Results\n\n"
            
            # Add MongoDB query in code block
            if "query" in nosql_data and nosql_data["query"] != "N/A":
                markdown += "**Executed MongoDB Query:**\n"
                markdown += f"```javascript\n{nosql_data['query']}\n```\n\n"
            
            # Add row count
            if "row_count" in nosql_data:
                markdown += f"**Documents Returned:** {nosql_data['row_count']}\n\n"
            
            # Add data in JSON code block
            if "data" in nosql_data and nosql_data["data"]:
                markdown += "**Results:**\n"
                markdown += f"```json\n{json.dumps(nosql_data['data'], indent=2, default=str)}\n```\n\n"
            else:
                markdown += "**Results:** No data returned\n\n"
        else:
            markdown += "## NoSQL Database Results\n\n"
            markdown += f"❌ **Error:** {nosql_data.get('error', 'Unknown NoSQL error')}\n\n"
    
    # Handle Data Engineer responses (for unclear queries)
    if "query_type" in results and results["query_type"] == "unclear":
        markdown += "## Response\n\n"
        if "response" in results:
            markdown += f"{results['response']}\n\n"
        
        # Add clarification suggestions if available
        if "clarification_suggestions" in results and results["clarification_suggestions"]:
            markdown += "### Clarification Suggestions\n\n"
            for i, suggestion in enumerate(results["clarification_suggestions"], 1):
                markdown += f"{i}. {suggestion}\n"
            markdown += "\n"
    
    # Handle hybrid results (combined from multiple sources)
    if "combined_data" in results:
        markdown += "## Combined Results\n\n"
        markdown += f"```json\n{json.dumps(results['combined_data'], indent=2, default=str)}\n```\n\n"
    
    # Add execution summary if available
    if "execution_path" in results:
        markdown += "## Execution Summary\n\n"
        markdown += f"**Processing Path:** {' → '.join(results['execution_path'])}\n\n"
    
    # Add raw data for debugging (collapsible)
    markdown += "<details>\n<summary>📋 Raw Response Data</summary>\n\n"
    markdown += f"```json\n{json.dumps(results, indent=2, default=str)}\n```\n\n"
    markdown += "</details>\n"
    
    return markdown

def format_error_response_markdown(error_msg: str, original_query: str) -> str:
    """Format error response in professional markdown"""
    
    markdown = f"# Query Error\n\n"
    markdown += f"**Original Query:** {original_query}\n\n"
    markdown += f"❌ **Error:** {error_msg}\n\n"
    
    # Add helpful suggestions
    markdown += "## Suggestions\n\n"
    markdown += "1. **Check your query syntax** - Ensure your question is clear and specific\n"
    markdown += "2. **Try rephrasing** - Use different words to describe what you're looking for\n"
    markdown += "3. **Be specific** - Instead of 'show everything', try 'show all employees' or 'list all products'\n"
    markdown += "4. **Check available data** - Ask about what data is available in the system\n\n"
    
    # Add example queries
    markdown += "## Example Queries\n\n"
    markdown += "- \"Show all employees in the IT department\"\n"
    markdown += "- \"List products with stock less than 10\"\n"
    markdown += "- \"Find employees with salary above $50,000\"\n"
    markdown += "- \"Show project completion status\"\n"
    markdown += "- \"What data is available in the system?\"\n\n"
    
    # Add timestamp
    markdown += f"**Timestamp:** {get_orchestrator()._get_timestamp()}\n"
    
    return markdown

def route_decision(state: OrchestratorState) -> str:
    """Decision function for routing based on domain"""
    # Initialize state if needed
    state = initialize_state(state)
    
    domain = state["query_domain"]
    
    if domain == QueryDomain.EMPLOYEE:
        return "sql_only"
    elif domain == QueryDomain.MOVIES:
        return "nosql_only"
    elif domain == QueryDomain.HYBRID:
        return "both_agents"
    elif domain == QueryDomain.UNCLEAR:
        return "data_engineer"  # Route unclear queries to Data Engineer Agent
    else:
        return "error_handling"

def sql_agent_decision(state: OrchestratorState) -> str:
    """Decision function for routing from SQL agent"""
    # Initialize state if needed
    state = initialize_state(state)
    
    domain = state["query_domain"]
    
    if domain == QueryDomain.HYBRID:
        return "nosql_agent"
    else:
        return "aggregate_results" 