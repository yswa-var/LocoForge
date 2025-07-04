"""
Modular SQL Agent Manager for LangGraph Studio
Provides robust SQL agent initialization and management
"""

import os
import json
import logging
from typing import Dict, Any, Optional, Union
from dotenv import load_dotenv
from functools import lru_cache
import traceback

# Set up logging
logger = logging.getLogger(__name__)

# Global SQL agent instance
_sql_agent_instance = None
_sql_agent_initialized = False

class SQLAgentManager:
    """Manages SQL agent initialization and provides a robust interface"""
    
    def __init__(self):
        self.agent = None
        self.initialized = False
        self.error_message = None
        self.last_error = None
        
    def initialize(self, force_reload: bool = False) -> bool:
        """
        Initialize the SQL agent with robust error handling
        
        Args:
            force_reload: Force re-initialization even if already initialized
            
        Returns:
            True if initialization successful, False otherwise
        """
        global _sql_agent_instance, _sql_agent_initialized
        
        # If already initialized and not forcing reload, return cached instance
        if _sql_agent_initialized and not force_reload and _sql_agent_instance:
            self.agent = _sql_agent_instance
            self.initialized = True
            return True
        
        try:
            # Load environment variables
            self._load_environment()
            
            # Check environment variables
            if not self._check_environment():
                return False
            
            # Import SQL agent
            SQLQueryExecutor = self._import_sql_agent()
            if not SQLQueryExecutor:
                return False
            
            # Initialize agent
            logger.info("🔄 Initializing SQL agent...")
            self.agent = SQLQueryExecutor()
            
            # Test connection
            if not self._test_connection():
                return False
            
            # Cache successful instance
            _sql_agent_instance = self.agent
            _sql_agent_initialized = True
            self.initialized = True
            
            logger.info("✅ SQL agent initialized successfully")
            return True
            
        except Exception as e:
            self.last_error = str(e)
            self.error_message = f"SQL agent initialization failed: {str(e)}"
            logger.error(f"❌ {self.error_message}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _load_environment(self):
        """Load environment variables with multiple fallback paths"""
        # Try multiple .env file locations
        env_paths = [
            '.env',
            '../.env', 
            '../../.env',
            os.path.join(os.path.dirname(__file__), '.env'),
            os.path.join(os.path.dirname(__file__), '../.env'),
            os.path.join(os.path.dirname(__file__), '../../.env')
        ]
        
        loaded = False
        for env_path in env_paths:
            if os.path.exists(env_path):
                load_dotenv(env_path, override=True)
                logger.info(f"Loaded environment from: {env_path}")
                loaded = True
        
        if not loaded:
            logger.warning("No .env file found, using system environment variables")
    
    def _check_environment(self) -> bool:
        """Check if required environment variables are available"""
        openai_key = os.getenv("OPENAPI_KEY") or os.getenv("OPENAI_API_KEY")
        postgres_url = os.getenv("POSTGRES_DB_URL")
        
        logger.info(f"Environment check - OPENAI_KEY: {'SET' if openai_key else 'NOT SET'}")
        logger.info(f"Environment check - POSTGRES_DB_URL: {'SET' if postgres_url else 'NOT SET'}")
        
        if not openai_key:
            self.error_message = "OpenAI API key not found. Please set OPENAPI_KEY or OPENAI_API_KEY"
            logger.error(f"❌ {self.error_message}")
            return False
        
        if not postgres_url:
            self.error_message = "PostgreSQL database URL not found. Please set POSTGRES_DB_URL"
            logger.error(f"❌ {self.error_message}")
            return False
        
        return True
    
    def _import_sql_agent(self):
        """Import SQL agent with error handling"""
        try:
            from my_agent.utils.sql_agent import SQLQueryExecutor
            return SQLQueryExecutor
        except ImportError as e:
            self.error_message = f"Failed to import SQL agent: {str(e)}"
            logger.error(f"❌ {self.error_message}")
            return None
        except Exception as e:
            self.error_message = f"Unexpected error importing SQL agent: {str(e)}"
            logger.error(f"❌ {self.error_message}")
            return None
    
    def _test_connection(self) -> bool:
        """Test SQL agent connection"""
        try:
            test_result = self.agent.execute_query("SELECT 1 as test")
            if test_result.get("success"):
                logger.info("✅ SQL agent connection test successful")
                return True
            else:
                self.error_message = f"SQL agent connection test failed: {test_result.get('error')}"
                logger.error(f"❌ {self.error_message}")
                return False
        except Exception as e:
            self.error_message = f"SQL agent connection test failed: {str(e)}"
            logger.error(f"❌ {self.error_message}")
            return False
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a SQL query using the agent
        
        Args:
            query: SQL query to execute
            
        Returns:
            Query result dictionary
        """
        if not self.initialized or not self.agent:
            return {
                "success": False,
                "error": "SQL agent not initialized",
                "data": [],
                "query": query
            }
        
        try:
            return self.agent.execute_query(query)
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "query": query
            }
    
    def generate_and_execute_query(self, prompt: str) -> Dict[str, Any]:
        """
        Generate SQL from natural language and execute it
        
        Args:
            prompt: Natural language prompt
            
        Returns:
            Generated SQL and execution result
        """
        if not self.initialized or not self.agent:
            return {
                "success": False,
                "error": "SQL agent not initialized",
                "data": [],
                "prompt": prompt
            }
        
        try:
            return self.agent.generate_and_execute_query(prompt)
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "prompt": prompt
            }
    
    def get_status(self) -> Dict[str, Any]:
        """Get SQL agent status"""
        return {
            "initialized": self.initialized,
            "agent_available": self.agent is not None,
            "error_message": self.error_message,
            "last_error": self.last_error
        }
    
    def reset(self):
        """Reset the SQL agent manager"""
        global _sql_agent_instance, _sql_agent_initialized
        self.agent = None
        self.initialized = False
        self.error_message = None
        self.last_error = None
        _sql_agent_instance = None
        _sql_agent_initialized = False
        logger.info("🔄 SQL agent manager reset")

# Global SQL agent manager instance
_sql_manager = None

def get_sql_manager() -> SQLAgentManager:
    """Get or create SQL agent manager instance"""
    global _sql_manager
    if _sql_manager is None:
        _sql_manager = SQLAgentManager()
    return _sql_manager

def initialize_sql_agent(force_reload: bool = False) -> bool:
    """
    Initialize SQL agent with robust error handling
    
    Args:
        force_reload: Force re-initialization
        
    Returns:
        True if successful, False otherwise
    """
    manager = get_sql_manager()
    return manager.initialize(force_reload)

def execute_sql_query(query: str) -> Dict[str, Any]:
    """
    Execute SQL query with automatic initialization
    
    Args:
        query: SQL query to execute
        
    Returns:
        Query result
    """
    manager = get_sql_manager()
    if not manager.initialized:
        if not manager.initialize():
            return {
                "success": False,
                "error": f"SQL agent initialization failed: {manager.error_message}",
                "data": [],
                "query": query
            }
    
    return manager.execute_query(query)

def generate_and_execute_sql(prompt: str) -> Dict[str, Any]:
    """
    Generate and execute SQL from natural language
    
    Args:
        prompt: Natural language prompt
        
    Returns:
        Generated SQL and execution result
    """
    manager = get_sql_manager()
    if not manager.initialized:
        if not manager.initialize():
            return {
                "success": False,
                "error": f"SQL agent initialization failed: {manager.error_message}",
                "data": [],
                "prompt": prompt
            }
    
    return manager.generate_and_execute_query(prompt)

def get_sql_agent_status() -> Dict[str, Any]:
    """Get SQL agent status"""
    manager = get_sql_manager()
    return manager.get_status()

def reset_sql_agent():
    """Reset SQL agent manager"""
    manager = get_sql_manager()
    manager.reset()

# Convenience function for backward compatibility
def create_sql_agent():
    """Create SQL agent (for backward compatibility)"""
    if initialize_sql_agent():
        manager = get_sql_manager()
        return manager.agent
    else:
        raise RuntimeError("Failed to initialize SQL agent")
