"""Define the nodes for our multi-agent system."""

from typing import Dict, Any, List
import json
from langchain_core.runnables import RunnableConfig
from langchain_core.messages import HumanMessage, AIMessage

from agent.state import State
from agent.configuration import Configuration
from agent.no_sql_agent import GeneralizedNoSQLAgent
from agent.sql_agent import SQLAgent
from agent.llm_config import llm

# Initialize agents
nosql_agent = GeneralizedNoSQLAgent()
sql_agent = SQLAgent("/Users/yash/Documents/langgraph_as/src/agent/sales.db")

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
        return {
            "error": f"Error fetching schemas: {str(e)}"
        }

async def supervisor_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """Supervisor node that decides which agent to delegate to based on schema context."""
    configuration = Configuration.from_runnable_config(config)
    
    # Get the last message
    last_message = state.messages[-1]["content"]
    
    # Get schema context
    schema_context = get_schema_context()
    
    # Create a prompt for the LLM to analyze the query and schemas
    system_prompt = f"""You are a database routing expert. Analyze the user's query and determine which database (SQL or NoSQL) is most appropriate to handle it.

SQL Schema:
{schema_context.get('sql_schema', 'No SQL schema available')}

NoSQL Schema:
{schema_context.get('nosql_schemas', 'No NoSQL schema available')}

User Query: {last_message}

Respond with a JSON object in this format:
{{
    "agent": "sql_agent" or "nosql_agent",
    "confidence": float between 0 and 1,
    "reasoning": "brief explanation"
}}"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": last_message}
    ]
    
    try:
        # Get routing decision from LLM
        response = llm.invoke(messages)
        routing_decision = json.loads(response.content)
        
        # Route based on confidence threshold
        if routing_decision["confidence"] >= 0.7:
            return {
                "current_agent": routing_decision["agent"],
                "task_type": "sql" if routing_decision["agent"] == "sql_agent" else "nosql",
                "reasoning": routing_decision["reasoning"]
            }
        else:
            # If confidence is low, try to determine based on keywords
            if any(keyword in last_message.lower() for keyword in ["sql", "table", "join", "select"]):
                return {"current_agent": "sql_agent", "task_type": "sql"}
            elif any(keyword in last_message.lower() for keyword in ["mongodb", "document", "collection"]):
                return {"current_agent": "nosql_agent", "task_type": "nosql"}
            else:
                return {"error": "Could not determine appropriate agent for the task"}
                
    except Exception as e:
        # Fallback to keyword-based routing if LLM fails
        if "sql" in last_message.lower() or "database" in last_message.lower():
            return {"current_agent": "sql_agent", "task_type": "sql"}
        elif "nosql" in last_message.lower() or "mongodb" in last_message.lower():
            return {"current_agent": "nosql_agent", "task_type": "nosql"}
        else:
            return {"error": "Could not determine appropriate agent for the task"}

async def sql_agent_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """SQL agent node for handling SQL database operations."""
    try:
        # Get the last message content
        last_message = state.messages[-1]["content"]
        
        # Execute the query using the SQL agent
        result = sql_agent.execute_query(last_message)
        
        return {
            "sql_result": result,
            "status": result.get("status", "error")
        }
    except Exception as e:
        return {
            "sql_result": None,
            "error": str(e),
            "status": "error"
        }

async def nosql_agent_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """NoSQL agent node for handling NoSQL database operations."""
    try:
        # Get the last message content
        last_message = state.messages[-1]["content"]
        
        # Execute the query using the NoSQL agent
        result = nosql_agent.execute_query(last_message)
        
        return {
            "nosql_result": result,
            "status": "success"
        }
    except Exception as e:
        return {
            "nosql_result": None,
            "error": str(e),
            "status": "error"
        }

async def drive_agent_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """Google Drive agent node for handling file operations."""
    # Implement Google Drive agent logic here
    return {"drive_result": "Drive operation result"} 