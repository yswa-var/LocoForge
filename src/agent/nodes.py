"""Define the nodes for our multi-agent system."""

from typing import Dict, Any, List
import json
import os
from langchain_core.runnables import RunnableConfig
from langchain_core.messages import HumanMessage, AIMessage

from agent.state import State
from agent.configuration import Configuration
from agent.no_sql_agent import GeneralizedNoSQLAgent
from agent.sql_agent import SQLAgent
from agent.llm_config import llm
from agent.google_drive_ops import DriveAgent
from agent.logger import default_logger as logger


# Initialize agents
nosql_agent = GeneralizedNoSQLAgent()

sql_db_path = os.getenv("SQL_DB_PATH", "/Users/yash/Documents/langgraph_as/src/agent/sales.db")
sql_agent = SQLAgent(sql_db_path)
logger.info(f"SQLAgent initialized with DB path: {sql_db_path}")

drive_credentials_path = os.getenv("GOOGLE_DRIVE_CREDENTIALS", "credentials.json")
drive_agent = DriveAgent(credentials_path=drive_credentials_path)
logger.info(f"DriveAgent initialized with credentials path: {drive_credentials_path}")

def get_schema_context() -> Dict[str, Any]:
    """Get schema information from both SQL and NoSQL databases."""
    try:
        # Get SQL schema
        sql_schema = sql_agent._get_table_schema()
        
        # Get NoSQL schema
        nosql_schemas = nosql_agent.get_all_schemas()
        
        return {
            "sql_schema": sql_schema,
            "nosql_schemas": nosql_schemas
        }
    except Exception as e:
        logger.error(f"Error fetching schemas: {str(e)}", exc_info=True)
        return {
            "error": f"Error fetching schemas: {str(e)}"
        }

async def supervisor_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """Supervisor node that decides which agent to delegate to based on schema context."""
    configuration = Configuration.from_runnable_config(config)
    
    logger.info("Supervisor node activated.")
    # Get the last message
    last_message_content = ""
    if state.messages:
        last_message_content = state.messages[-1].get("content", "") if isinstance(state.messages[-1], dict) else state.messages[-1].content

    logger.info(f"Last message for supervisor: {last_message_content}")
    
    schema_context = get_schema_context()
    if "error" in schema_context:
        logger.warning(f"Schema context could not be fully determined: {schema_context['error']}")

    # Create a prompt for the LLM to analyze the query and schemas
    system_prompt = f"""You are a routing expert. Analyze the user's query and determine which agent (sql_agent, nosql_agent, or drive_agent) is most appropriate to handle it.
    - sql_agent: Handles SQL database queries (schema below).
    - nosql_agent: Handles NoSQL database queries (e.g., MongoDB, schema below).
    - drive_agent: Handles Google Drive operations like listing files, uploading, downlaoding, searching, creating folders, deleting files.

SQL Schema:
{schema_context.get('sql_schema', 'No SQL schema available')}

NoSQL Schema:
{schema_context.get('nosql_schemas', 'No NoSQL schema available')}

User Query: {last_message_content}

Respond with a JSON object in this format:
{{
    "agent": "sql_agent" or "nosql_agent" or "drive_agent",
    "confidence": float between 0 and 1,
    "reasoning": "brief explanation"
}}"""

    messages_for_llm = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": last_message_content}
    ]
    
    routing_decision_str = "" # Initialize for potential use in error logging
    try:
        logger.info("Invoking LLM for routing decision.")
        # Note: llm.invoke is a synchronous call.
        response = llm.invoke(messages_for_llm)
        routing_decision_str = response.content
        logger.debug(f"LLM response for routing: {routing_decision_str}")
        routing_decision = json.loads(routing_decision_str)
        
        agent_choice = routing_decision.get("agent")
        confidence = routing_decision.get("confidence", 0)
        reasoning = routing_decision.get("reasoning", "")
        
        logger.info(f"LLM routing decision: Agent={agent_choice}, Confidence={confidence}, Reasoning='{reasoning}'")

        if agent_choice in ["sql_agent", "nosql_agent", "drive_agent"] and confidence > 0.7: # Using > 0.7 as per original
            task_type = "unknown"
            if agent_choice == "sql_agent" :
                task_type = "sql"
            elif agent_choice == "nosql_agent":
                task_type = "nosql"
            elif agent_choice == "drive_agent":
                task_type = "drive"
            
            logger.info(f"Routing to {agent_choice} based on LLM (confidence: {confidence}).")
            return {
                "current_agent": agent_choice,
                "task_type": task_type,
                "reasoning": reasoning
            }
        else:
            logger.info("LLM confidence below threshold or agent not recognized, falling back to keyword routing.")
            if any(keyword in last_message_content.lower() for keyword in ["sql", "table", "join", "select", "database query"]):
                logger.info("Keyword-based routing to sql_agent.")
                return {"current_agent": "sql_agent", "task_type": "sql", "reasoning": "Keyword-based routing to SQL."}
            elif any(keyword in last_message_content.lower() for keyword in ["mongodb", "document", "collection", "nosql query"]):
                logger.info("Keyword-based routing to nosql_agent.")
                return {"current_agent": "nosql_agent", "task_type": "nosql", "reasoning": "Keyword-based routing to NoSQL."}
            elif any(keyword in last_message_content.lower() for keyword in ["drive", "file", "folder", "upload", "download", "google drive", "list files", "search files"]):
                logger.info("Keyword-based routing to drive_agent.")
                return {"current_agent": "drive_agent", "task_type": "drive", "reasoning": "Keyword-based routing to Drive."}
            else:
                logger.warning("Could not determine appropriate agent via LLM or keywords.")
                return {"error": "Could not determine appropriate agent for the task based on LLM or keywords."}
                
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM routing response: {str(e)}. Response was: {routing_decision_str}", exc_info=True)
        return {"error": f"LLM response parsing error: {str(e)}"}
    except Exception as e:
        logger.error(f"Error in supervisor_node during LLM routing: {str(e)}", exc_info=True)
        logger.info("LLM failed, attempting keyword-based routing as fallback.")
        if any(keyword in last_message_content.lower() for keyword in ["sql", "table", "join", "select", "database query"]):
           logger.info("LLM failed, keyword-based routing to sql_agent.")
           return {"current_agent": "sql_agent", "task_type": "sql", "reasoning": "LLM failed, keyword-based routing to SQL."}
        elif any(keyword in last_message_content.lower() for keyword in ["mongodb", "document", "collection", "nosql query"]):
            logger.info("LLM failed, keyword-based routing to nosql_agent.")
            return {"current_agent": "nosql_agent", "task_type": "nosql", "reasoning": "LLM failed, keyword-based routing to NoSQL."}
        elif any(keyword in last_message_content.lower() for keyword in ["drive", "file", "folder", "upload", "download", "google drive", "list files", "search files"]):
            logger.info("LLM failed, keyword-based routing to drive_agent.")
            return {"current_agent": "drive_agent", "task_type": "drive", "reasoning": "LLM failed, keyword-based routing to Drive."}
        else:
            logger.error("LLM failed, and no keyword match found for routing.")
            return {"error": f"Could not determine appropriate agent for the task. LLM Error: {str(e)}"}

async def sql_agent_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """SQL agent node for handling SQL database operations."""
    logger.info("SQL agent node activated.")
    last_message_content = ""
    if state.messages:
        last_message_content = state.messages[-1].get("content", "") if isinstance(state.messages[-1], dict) else state.messages[-1].content
    logger.info(f"Executing SQL query: {last_message_content}")
    try:
        # Note: sql_agent.execute_query is a synchronous call.
        result = sql_agent.execute_query(last_message_content)
        logger.info(f"SQL query execution result: {result}")
        return {
            "sql_result": result,
            "status": result.get("status", "success") if isinstance(result, dict) else "success" # Keep original logic
        }
    except Exception as e:
        logger.error(f"Error in sql_agent_node: {str(e)}", exc_info=True)
        return {
            "sql_result": None,
            "error": str(e),
            "status": "error"
        }

async def nosql_agent_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """NoSQL agent node for handling NoSQL database operations."""
    logger.info("NoSQL agent node activated.")
    last_message_content = ""
    if state.messages:
        last_message_content = state.messages[-1].get("content", "") if isinstance(state.messages[-1], dict) else state.messages[-1].content
    logger.info(f"Executing NoSQL query: {last_message_content}")
    try:
        # Note: nosql_agent.execute_query is a synchronous call.
        result = nosql_agent.execute_query(last_message_content)
        logger.info(f"NoSQL query execution result: {result}")
        return {
            "nosql_result": result,
            "status": result.get("status", "success") if isinstance(result, dict) else "success"
        }
    except Exception as e:
        logger.error(f"Error in nosql_agent_node: {str(e)}", exc_info=True)
        return {
            "nosql_result": None,
            "error": str(e),
            "status": "error"
        }

async def drive_agent_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """Google Drive agent node for handling file operations."""
    logger.info("Google Drive agent node activated.")
    last_message_content = ""
    if state.messages:
        last_message_content = state.messages[-1].get("content", "") if isinstance(state.messages[-1], dict) else state.messages[-1].content
    logger.info(f"Executing Drive command: {last_message_content}")
    
    result_json_str = "" # Initialize for use in except block
    try:
        # drive_agent.execute_command is already async as per google_drive_ops.py
        result_json_str = await drive_agent.execute_command(last_message_content)
        logger.debug(f"Drive agent raw response: {result_json_str}")
        result = json.loads(result_json_str)
        logger.info(f"Drive command execution result: {result}")

        if "error" in result:
            logger.error(f"Error reported by DriveAgent: {result['error']}")
            return {
                "drive_result": None, # Or result if you want to pass partial errors
                "error": result["error"],
                "status": "error"
            }
        else: 
            return {
                "drive_result": result,
                "status": result.get("status", "success")
            }
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse DriveAgent response: {str(e)}. Response was: {result_json_str}", exc_info=True)
        return {
            "drive_result": None,
            "error": f"Failed to parse DriveAgent response: {str(e)}. Response was: {result_json_str}",
            "status": "error"
        }
    except Exception as e:
        logger.error(f"Error in drive_agent_node: {str(e)}", exc_info=True)
        return {
            "drive_result": None,
            "error": str(e),
            "status":"error"
        }