from typing import Dict, Any, List, Optional, Union
from sql_agent import SQLAgent
from nosql_agent import GeneralizedNoSQLAgent, MongoJSONEncoder
from llm_config import llm
from logger import default_logger as logger
import json

class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    def __init__(self, message: str, guidance: str = None):
        self.message = message
        self.guidance = guidance
        super().__init__(self.message)

def format_error_response(error: Exception, guidance: str = None) -> Dict[str, Any]:
    """Format error response with helpful guidance."""
    return {
        "status": "error",
        "error": str(error),
        "guidance": guidance or "Please check your request and try again.",
        "help": "For more information, please refer to the documentation or contact support."
    }

def process_message(messages: list) -> str:
    """Process messages and return LLM response."""
    logger.debug("Processing LLM messages")
    try:
        response = llm.invoke(messages)
        return response.content
    except Exception as e:
        logger.error(f"Error processing LLM message: {str(e)}", exc_info=True)
        raise DatabaseError(
            "Failed to process your request",
            "Please ensure your request is clear and try again. If the problem persists, contact support."
        )

def task_creation_agent(objective: str) -> Dict[str, Any]:
    """
    This function takes an objective and returns a structured task specification.
    
    Args:
        objective (str): The natural language objective to be accomplished
        
    Returns:
        Dict[str, Any]: A structured task specification including:
            - agent_type: "sql" or "nosql" or "both"
            - task_type: The type of operation (e.g., "query", "insert", "update", "delete")
            - prompt: The natural language prompt for the specific agent
            - metadata: Additional context or requirements
    """
    if not objective or objective.strip() == "":
        raise DatabaseError(
            "Empty request received",
            "Please provide a specific task or question you'd like help with."
        )

    logger.info(f"Creating task specification for objective: {objective}")
    system_prompt = """You are a task routing expert. Your job is to analyze database-related objectives and determine which database agent should handle the task based on the following strict rules:

ROUTING RULES:
1. Use NoSQL agent (mongodb) for ALL user-related data including:
   * User profiles and authentication
   * User roles and permissions
   * User activity logs
   * User preferences and settings
   * User sessions and tokens
   * User notifications
   * User feedback and ratings
   * User-generated content
   * User analytics and tracking
   * User relationships and connections

2. Use SQL agent for ALL business and sales-related data including:
   * Sales transactions and orders
   * Product catalog and inventory
   * Customer business information
   * Financial records and transactions
   * Business analytics and reporting
   * Pricing and discounts
   * Business metrics and KPIs
   * Supply chain and logistics
   * Business relationships and partnerships
   * Business documents and contracts

3. Use both agents ONLY when:
   * The task explicitly requires combining user data with business data
   * The operation needs to update both user and business records
   * The query needs to join user information with business data

Return a JSON object with this structure:
{
    "agent_type": "sql" | "nosql" | "both",
    "task_type": "query" | "insert" | "update" | "delete" | "schema" | "management",
    "prompt": "The natural language prompt for the agent",
    "metadata": {
        "reasoning": "Explanation of why this agent was chosen based on the routing rules",
        "data_category": "user" | "business" | "both",
        "requirements": ["List of specific requirements"],
        "constraints": ["List of any constraints"]
    }
}

IMPORTANT: Always analyze the objective carefully to determine if it's primarily about user data or business data."""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": objective}
    ]
    
    try:
        response = llm.invoke(messages)
        task_spec = json.loads(response.content)
        logger.info(f"Successfully created task specification: {json.dumps(task_spec, indent=2)}")
        return task_spec
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse task specification: {str(e)}", exc_info=True)
        raise DatabaseError(
            "Failed to understand your request",
            "Please rephrase your request in a clearer way. For example:\n"
            "- 'Show me all products with low inventory'\n"
            "- 'Find users who made recent purchases'\n"
            "- 'Update the price of product X'"
        )
    except Exception as e:
        logger.error(f"Unexpected error in task creation: {str(e)}", exc_info=True)
        raise DatabaseError(
            "An unexpected error occurred",
            "Please try again. If the problem persists, contact support."
        )

def execute_task(task_spec: Dict[str, Any], sql_agent: Optional[SQLAgent] = None, nosql_agent: Optional[GeneralizedNoSQLAgent] = None) -> Dict[str, Any]:
    """
    Execute a task using the specified agent(s).
    
    Args:
        task_spec (Dict[str, Any]): The task specification from task_creation_agent
        sql_agent (Optional[SQLAgent]): The SQL agent instance
        nosql_agent (Optional[GeneralizedNoSQLAgent]): The NoSQL agent instance
        
    Returns:
        Dict[str, Any]: Results from the executed task(s)
    """
    logger.info(f"Executing task with specification: {json.dumps(task_spec, indent=2, cls=MongoJSONEncoder)}")
    agent_type = task_spec["agent_type"]
    results = {}
    
    try:
        if agent_type in ["sql", "both"]:
            if not sql_agent:
                raise DatabaseError(
                    "SQL database not available",
                    "Please ensure the SQL database is properly configured and try again."
                )
            logger.info("Executing SQL agent task")
            results["sql"] = sql_agent.execute_query(task_spec["prompt"])
        
        if agent_type in ["nosql", "both"]:
            if not nosql_agent:
                raise DatabaseError(
                    "NoSQL database not available",
                    "Please ensure the NoSQL database is properly configured and try again."
                )
            logger.info("Executing NoSQL agent task")
            
            # Check if database is selected
            try:
                # Try to access the database name - this will raise an error if no database is selected
                _ = nosql_agent.current_db.name
            except (AttributeError, TypeError):
                # Try to use a default database
                default_db = "user_management"  # or any other appropriate default database
                logger.info(f"No database selected. Attempting to use default database: {default_db}")
                if not nosql_agent.use_database(default_db):
                    raise DatabaseError(
                        "No database selected",
                        f"Please select a database first. Available databases: {nosql_agent.list_databases()}\n"
                        f"Example: 'use database {default_db}'"
                    )
            
            results["nosql"] = nosql_agent.execute_query(task_spec["prompt"])
        
        # Check for errors in results
        for agent, result in results.items():
            if result.get("status") == "error":
                error_msg = result.get("error", result.get("message", "Unknown error"))
                if "syntax error" in error_msg.lower():
                    raise DatabaseError(
                        f"Invalid query syntax in {agent} database",
                        "Please check your request and ensure it follows the correct format."
                    )
                elif "no database selected" in error_msg.lower():
                    available_dbs = nosql_agent.list_databases() if agent == "nosql" else []
                    raise DatabaseError(
                        f"Database not selected in {agent}",
                        f"Please select a database first. Available databases: {available_dbs}\n"
                        f"Example: 'use database user_management'"
                    )
                else:
                    raise DatabaseError(
                        f"Error in {agent} database: {error_msg}",
                        "Please check your request and try again."
                    )
        
        logger.info("Task execution completed successfully")
        return {
            "status": "success",
            "task_spec": task_spec,
            "results": results
        }
        
    except DatabaseError as e:
        logger.error(f"Database error: {str(e)}", exc_info=True)
        return format_error_response(e, e.guidance)
    except Exception as e:
        logger.error(f"Unexpected error during task execution: {str(e)}", exc_info=True)
        return format_error_response(
            e,
            "An unexpected error occurred. Please try again or contact support if the problem persists."
        )

def main():
    """Example usage of the task router."""
    logger.info("Starting LLM router example")
    
    # Example objectives
    objectives = [
        # User-related queries (NoSQL)
        "Find all users who logged in within the last 24 hours",
        "Update the role of user 'john.doe@example.com' to 'admin'",
        "Get the activity log for user ID '12345'",
        
        # Business-related queries (SQL)
        "Show me total sales for the last quarter",
        "List all products with inventory below 100 units",
        "Calculate the average order value by customer",
        
        # Combined queries (Both)
        "Find all users who made purchases above $1000 and their purchase history",
        "Get user feedback for products with low inventory"
    ]
    
    try:
        # Initialize agents (in a real application, these would be properly configured)
        logger.info("Initializing database agents")
        sql_agent = SQLAgent("sales.db")
        nosql_agent = GeneralizedNoSQLAgent()
        
        for objective in objectives:
            logger.info(f"Processing objective: {objective}")
            print(f"\nProcessing objective: {objective}")
            
            try:
                # Create task specification
                task_spec = task_creation_agent(objective)
                print("\nTask Specification:")
                print(json.dumps(task_spec, indent=2))
                
                # Execute task
                result = execute_task(task_spec, sql_agent, nosql_agent)
                print("\nExecution Results:")
                print(json.dumps(result, indent=2))
                
            except DatabaseError as e:
                print(f"\nError: {e.message}")
                print(f"Guidance: {e.guidance}")
            except Exception as e:
                print(f"\nUnexpected error: {str(e)}")
                print("Please try again or contact support if the problem persists.")
                
    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}", exc_info=True)
        print(f"An error occurred: {str(e)}")
    finally:
        logger.info("Closing database connections")
        if 'sql_agent' in locals():
            sql_agent.close()
        if 'nosql_agent' in locals():
            nosql_agent.close()

if __name__ == "__main__":
    main()

