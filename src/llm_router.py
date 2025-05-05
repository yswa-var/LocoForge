from typing import Dict, Any, List, Optional, Union
from sql_agent import SQLAgent
from nosql_agent import GeneralizedNoSQLAgent, MongoJSONEncoder
from google_drive_ops import DriveAgent
from llm_config import llm
# from llm_utils import process_message //It is already defined inside the file 
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
    Create a task specification from the user's objective.
    
    Args:
        objective (str): The user's objective in natural language
        
    Returns:
        Dict[str, Any]: Task specification
    """
    system_prompt = """You are a task creation agent that converts natural language objectives into structured task specifications.
    Your response must be a valid JSON object with the following structure:
    {
        "agent_type": "sql" | "nosql" | "drive" | "both",
        "task_type": "query" | "update" | "delete" | "insert",
        "prompt": "The original user objective"
    }
    
    Guidelines:
    1. Choose the appropriate agent_type based on the task:
       - Use "sql" for structured data queries and operations
       - Use "nosql" for document-based or unstructured data
       - Use "drive" for Google Drive operations
       - Use "both" when the task requires multiple types of databases
    2. Choose the appropriate task_type based on the operation:
       - "query" for SELECT, FIND, or LIST operations
       - "update" for UPDATE or MODIFY operations
       - "delete" for DELETE or REMOVE operations
       - "insert" for INSERT, CREATE, or UPLOAD operations
    3. Keep the prompt as close to the original objective as possible
    4. Ensure the response is valid JSON
    """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": objective}
    ]
    
    try:
        response = llm.invoke(messages)
        logger.debug(f"Raw LLM response: {response.content}")
        
        try:
            task_spec = json.loads(response.content)
            logger.info(f"Successfully created task specification: {json.dumps(task_spec, indent=2)}")
            
            # Validate required fields
            required_fields = ["agent_type", "task_type", "prompt"]
            missing_fields = [field for field in required_fields if field not in task_spec]
            if missing_fields:
                raise ValueError(f"Missing required fields in task specification: {missing_fields}")
            
            # Validate agent_type
            valid_agent_types = ["sql", "nosql", "drive", "both"]
            if task_spec["agent_type"] not in valid_agent_types:
                raise ValueError(f"Invalid agent_type: {task_spec['agent_type']}. Must be one of {valid_agent_types}")
            
            # Validate task_type
            valid_task_types = ["query", "update", "delete", "insert"]
            if task_spec["task_type"] not in valid_task_types:
                raise ValueError(f"Invalid task_type: {task_spec['task_type']}. Must be one of {valid_task_types}")
            
            return task_spec
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse task specification: {str(e)}")
            logger.error(f"Raw response content: {response.content}")
            raise DatabaseError(
                "Failed to understand your request",
                "Please rephrase your request in a clearer way. For example:\n"
                "- 'Show me all products with low inventory'\n"
                "- 'Find users who made recent purchases'\n"
                "- 'Update the price of product X'\n"
                "- 'List files in my Google Drive'\n"
                "- 'Create a new folder in Google Drive'"
            )
    except Exception as e:
        logger.error(f"Unexpected error in task creation: {str(e)}", exc_info=True)
        raise DatabaseError(
            "An unexpected error occurred",
            "Please try again. If the problem persists, contact support."
        )

async def execute_task(task_spec: Dict[str, Any], sql_agent: Optional[SQLAgent] = None, 
                nosql_agent: Optional[GeneralizedNoSQLAgent] = None,
                drive_agent: Optional[DriveAgent] = None) -> Dict[str, Any]:
    """
    Execute a task using the specified agent(s).
    
    Args:
        task_spec (Dict[str, Any]): The task specification from task_creation_agent
        sql_agent (Optional[SQLAgent]): The SQL agent instance
        nosql_agent (Optional[GeneralizedNoSQLAgent]): The NoSQL agent instance
        drive_agent (Optional[DriveAgent]): The Google Drive agent instance
        
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
            
        if agent_type in ["drive", "both"]:
            if not drive_agent:
                raise DatabaseError(
                    "Google Drive not available",
                    "Please ensure Google Drive is properly configured and try again."
                )
            logger.info("Executing Google Drive agent task")
            drive_result = await drive_agent.execute_command(task_spec["prompt"])
            
            # Parse JSON if needed
            if isinstance(drive_result, str):
                try:
                    results["drive"] = json.loads(drive_result)
                except json.JSONDecodeError:
                    results["drive"] = {"result": drive_result}
            else:
                results["drive"] = drive_result
        # Check for errors in results
        for agent, result in results.items():
            if isinstance(result, str):
                try:
                    result = json.loads(result)
                    results[agent] = result  # Replace with parsed JSON
                except json.JSONDecodeError:
                    # Not JSON, keep as is
                    continue
            if isinstance(result, dict) and result.get("status") == "error":
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
        logger.error(f"Unexpected error in task execution: {str(e)}", exc_info=True)
        return format_error_response(e)

async def main():
    """Main function to demonstrate the workflow."""
    # Example objectives
    objectives = [
        # SQL queries
        "Show me total sales for the last quarter",
        "List all products with inventory below 100 units",
        
        # NoSQL queries
        "Find all users who logged in within the last 24 hours",
        "Update the role of user 'john.doe@example.com' to 'admin'",
        
        # Combined queries
        "Find all users who made purchases above $1000 and their purchase history",
        "Get user feedback for products with low inventory",
        
        # Google Drive operations
        "List files in my Google Drive",
        "Create a new folder in Google Drive"
    ]
    
    try:
        # Initialize agents
        sql_agent = SQLAgent("sales.db")
        nosql_agent = GeneralizedNoSQLAgent()
        drive_agent = DriveAgent()
        
        for objective in objectives:
            print(f"\n{'='*50}")
            print(f"Processing objective: {objective}")
            print(f"{'='*50}")
            
            try:
                # Create task specification
                task_spec = task_creation_agent(objective)
                print("\nTask Specification:")
                print(json.dumps(task_spec, indent=2))
                
                # Execute task
                result = await execute_task(task_spec, sql_agent, nosql_agent, drive_agent)
                print("\nExecution Results:")
                print(json.dumps(result, indent=2))
                
            except Exception as e:
                print(f"\nError processing objective: {str(e)}")
                continue
                
    finally:
        # Cleanup
        if 'sql_agent' in locals():
            sql_agent.close()
        if 'nosql_agent' in locals():
            nosql_agent.close()
        if 'drive_agent' in locals():
            await drive_agent.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

