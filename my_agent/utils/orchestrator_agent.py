"""
Hybrid Graph-Based Workflow Orchestrator
Combines Intent Classification, Query Decomposition, Result Aggregation, and Context Management
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from my_agent.utils.state import OrchestratorState, QueryDomain, QueryIntent
from my_agent.utils.nosql_agent import NoSQLQueryExecutor

# Set up logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Import agents with error handling
try:
    from my_agent.utils.sql_agent import SQLQueryExecutor
    SQL_AVAILABLE = True
except ImportError as e:
    print(f"Warning: SQL agent not available: {e}")
    SQL_AVAILABLE = False

try:
    from my_agent.utils.nosql_agent import NoSQLQueryExecutor
    NOSQL_AVAILABLE = True
except ImportError as e:
    print(f"Warning: NoSQL agent not available: {e}")
    NOSQL_AVAILABLE = False

class HybridOrchestrator:
    """Hybrid Orchestrator that manages SQL and NoSQL agents with intelligent routing"""
    
    def __init__(self):
        """Initialize the orchestrator with both agents"""
        # Initialize agents only if available
        self.sql_agent = None
        self.nosql_agent = None
        
        # Load environment variables explicitly with multiple attempts
        load_dotenv()
        
        # Additional environment loading for LangGraph Studio
        env_file_paths = ['.env', '../.env', '../../.env']
        for env_path in env_file_paths:
            if os.path.exists(env_path):
                load_dotenv(env_path)
                logger.info(f"Loaded environment from: {env_path}")
        
        # Verify environment variables
        mongo_db = "mongodb://localhost:27017/"
        openai_key = os.getenv("OPENAI_API_KEY")
        sql_db = os.getenv("SQL_DB")
        
        logger.info(f"Environment check - MONGO_DB: {'SET' if mongo_db else 'NOT SET'}")
        logger.info(f"Environment check - OPENAI_API_KEY: {'SET' if openai_key else 'NOT SET'}")
        logger.info(f"Environment check - SQL_DB: {'SET' if sql_db else 'NOT SET'}")
        
        if SQL_AVAILABLE:
            try:
                self.sql_agent = SQLQueryExecutor()
                logger.info("✅ SQL agent initialized successfully")
            except Exception as e:
                logger.warning(f"Warning: Failed to initialize SQL agent: {e}")
                logger.warning(f"Error details: {type(e).__name__}: {str(e)}")
        
        if NOSQL_AVAILABLE:
            try:
                # Check if MongoDB connection string is available
                if not mongo_db:
                    logger.warning("Warning: MONGO_DB environment variable not set")
                    logger.warning("Available environment variables: " + str([k for k in os.environ.keys() if 'MONGO' in k or 'DB' in k]))
                else:
                    logger.info(f"Initializing NoSQL agent with connection: {mongo_db}")
                    self.nosql_agent = NoSQLQueryExecutor()
                    logger.info("✅ NoSQL agent initialized successfully")
            except Exception as e:
                logger.warning(f"Warning: Failed to initialize NoSQL agent: {e}")
                logger.warning(f"Error details: {type(e).__name__}: {str(e)}")
                import traceback
                logger.warning(f"Full traceback: {traceback.format_exc()}")
        else:
            logger.warning("Warning: NoSQL agent not available (import failed)")
        
        self.model = ChatOpenAI(
            model="gpt-4o-mini",
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        
        # Domain keywords for classification
        self.domain_keywords = {
            QueryDomain.EMPLOYEE: [
                "employee", "staff", "department", "salary", "manager", "hire", 
                "attendance", "project", "position", "budget", "performance"
            ],
            QueryDomain.WAREHOUSE: [
                "product", "inventory", "stock", "order", "supplier", "warehouse",
                "batch", "expiry", "reorder", "movement", "customer", "delivery"
            ]
        }
        
        # Context window for conversation history
        self.context_window = 5
    
    def classify_intent(self, query: str) -> Tuple[QueryDomain, QueryIntent]:
        """
        Use LLM to classify query domain and intent
        
        Args:
            query: User query string
            
        Returns:
            Tuple of (QueryDomain, QueryIntent)
        """
        system_prompt = """
You are an expert query classifier for a hybrid database system with:
1. SQL Database: Employee management (employees, departments, projects, attendance)
2. NoSQL Database: Grocery warehouse (products, inventory, orders, suppliers)

Classify the query into:
- DOMAIN: employee, warehouse, hybrid, unknown
- INTENT: select, analyze, compare, aggregate

Return ONLY a JSON object with "domain" and "intent" fields.
"""
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Classify this query: {query}")
        ]
        
        response = self.model.invoke(messages)
        
        try:
            classification = json.loads(response.content)
            domain = QueryDomain(classification.get("domain", "unknown"))
            intent = QueryIntent(classification.get("intent", "select"))
            return domain, intent
        except:
            return QueryDomain.UNKNOWN, QueryIntent.SELECT
    
    def decompose_query(self, query: str, domain: QueryDomain) -> Dict[str, str]:
        """
        Decompose complex queries into sub-queries for each domain
        
        Args:
            query: Original query
            domain: Classified domain
            
        Returns:
            Dictionary of sub-queries for each domain
        """
        if domain == QueryDomain.HYBRID:
            return self._decompose_hybrid_query(query)
        else:
            return {domain.value: query}
    
    def _decompose_hybrid_query(self, query: str) -> Dict[str, str]:
        """Decompose hybrid queries into domain-specific sub-queries"""
        system_prompt = """
Decompose this hybrid query into separate sub-queries for:
1. SQL (employee data): employees, departments, projects, attendance
2. NoSQL (warehouse data): products, inventory, orders, suppliers

Return JSON with "sql" and "nosql" fields containing the sub-queries.
"""
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Decompose: {query}")
        ]
        
        response = self.model.invoke(messages)
        
        try:
            decomposition = json.loads(response.content)
            return {
                "sql": decomposition.get("sql", ""),
                "nosql": decomposition.get("nosql", "")
            }
        except:
            return {"sql": query, "nosql": query}
    
    def execute_sql_query(self, query: str) -> Dict[str, Any]:
        """Execute SQL query using SQL agent"""
        if self.sql_agent is None:
            return {"success": False, "error": "SQL agent is not initialized. Check your environment variables and database setup.", "data": []}
        try:
            return self.sql_agent.generate_and_execute_query(query)
        except Exception as e:
            return {"success": False, "error": str(e), "data": []}
    
    def execute_nosql_query(self, query: str) -> Dict[str, Any]:
        """Execute NoSQL query using NoSQL agent"""
        if self.nosql_agent is None:
            return {"success": False, "error": "NoSQL agent is not initialized. Check your environment variables and MongoDB setup.", "data": []}
        try:
            return self.nosql_agent.generate_and_execute_query(query)
        except Exception as e:
            return {"success": False, "error": str(e), "data": []}
    
    def aggregate_results(self, sql_results: Dict[str, Any], 
                         nosql_results: Dict[str, Any], 
                         original_query: str) -> Dict[str, Any]:
        """
        Format results from both agents as JSON without LLM processing
        
        Args:
            sql_results: Results from SQL agent
            nosql_results: Results from NoSQL agent
            original_query: Original user query
            
        Returns:
            Formatted JSON results
        """
        # Check if both results are successful
        sql_success = sql_results.get("execution_result", {}).get("success", False) if sql_results else False
        nosql_success = nosql_results.get("execution_result", {}).get("success", False) if nosql_results else False
        
        # If both failed, return error
        if not sql_success and not nosql_success:
            return {
                "success": False,
                "error": "Both SQL and NoSQL queries failed",
                "sql_error": sql_results.get("execution_result", {}).get("error", "Unknown error") if sql_results else "No SQL results",
                "nosql_error": nosql_results.get("execution_result", {}).get("error", "Unknown error") if nosql_results else "No NoSQL results"
            }
        
        # Prepare the combined results
        combined_data = {
            "original_query": original_query,
            "timestamp": self._get_timestamp(),
            "data_sources": []
        }
        
        # Add SQL results if successful
        if sql_success:
            combined_data["sql_data"] = {
                "success": True,
                "query": sql_results.get("generated_sql", "N/A"),
                "row_count": sql_results.get("execution_result", {}).get("row_count", 0),
                "data": sql_results.get("execution_result", {}).get("data", [])
            }
            combined_data["data_sources"].append("sql")
        else:
            combined_data["sql_data"] = {
                "success": False,
                "error": sql_results.get("execution_result", {}).get("error", "Unknown error") if sql_results else "No SQL results"
            }
        
        # Add NoSQL results if successful
        if nosql_success:
            execution_result = nosql_results.get("execution_result", {})
            combined_data["nosql_data"] = {
                "success": True,
                "query": nosql_results.get("generated_mongodb_query", "N/A"),
                "row_count": execution_result.get("row_count", 0),
                "data": execution_result.get("data", [])
            }
            combined_data["data_sources"].append("nosql")
        else:
            combined_data["nosql_data"] = {
                "success": False,
                "error": nosql_results.get("execution_result", {}).get("error", "Unknown error") if nosql_results else "No NoSQL results"
            }
        
        # Set overall success
        combined_data["success"] = sql_success or nosql_success
        
        return combined_data
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def update_context(self, state: OrchestratorState) -> OrchestratorState:
        """Update conversation context for better routing"""
        context_entry = {
            "query": state["current_query"],
            "domain": state["query_domain"].value,
            "intent": state["query_intent"].value,
            "execution_path": state["execution_path"]
        }
        
        context_history = state.get("context_history", [])
        context_history.append(context_entry)
        
        # Keep only recent context
        if len(context_history) > self.context_window:
            context_history = context_history[-self.context_window:]
        
        state["context_history"] = context_history
        return state
    
    def get_context_summary(self, context_history: List[Dict[str, Any]]) -> str:
        """Generate context summary for better routing"""
        if not context_history:
            return ""
        
        recent_contexts = context_history[-3:]  # Last 3 interactions
        summary = "Recent context:\n"
        
        for ctx in recent_contexts:
            summary += f"- Query: {ctx['query'][:50]}... (Domain: {ctx['domain']})\n"
        
        return summary
    
    def check_agent_status(self) -> Dict[str, Any]:
        """Check the status of both agents and return detailed information"""
        status = {
            "sql_agent": {
                "available": SQL_AVAILABLE,
                "initialized": self.sql_agent is not None,
                "status": "✅ Ready" if self.sql_agent else "❌ Not initialized"
            },
            "nosql_agent": {
                "available": NOSQL_AVAILABLE,
                "initialized": self.nosql_agent is not None,
                "status": "✅ Ready" if self.nosql_agent else "❌ Not initialized"
            },
            "environment": {
                "mongo_db": "mongodb://localhost:27017/",
                "openai_key": "SET" if os.getenv("OPENAI_API_KEY") else "NOT SET",
                "sql_db": os.getenv("SQL_DB", "NOT SET")
            }
        }
        
        # Add detailed error information if agents failed to initialize
        if not self.nosql_agent and NOSQL_AVAILABLE:
            status["nosql_agent"]["error"] = "Agent import succeeded but initialization failed"
            if not os.getenv("MONGO_DB"):
                status["nosql_agent"]["error"] = "MONGO_DB environment variable not set"
        
        if not self.sql_agent and SQL_AVAILABLE:
            status["sql_agent"]["error"] = "Agent import succeeded but initialization failed"
            if not os.getenv("SQL_DB"):
                status["sql_agent"]["error"] = "SQL_DB environment variable not set"
        
        return status

