"""Multi-Database Query Orchestrator Graph Implementation.

Implements a three-layer architecture for robust query processing:
1. Supervisor Layer - Query analysis and task breakdown
2. Router Layer - Task routing to appropriate agent
3. Database Agent Layer - SQL, NoSQL, and Drive execution agents
"""

from __future__ import annotations

from typing import Any, Dict, Union, List
from loguru import logger
import os
import asyncio

from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END

from src.agent.types import State, Task, TaskStatus, Configuration, DatabaseType
from src.agent.supervisor_node import supervisor_node
from src.agent.sql_agent import SQLAgent
from src.agent.no_sql_agent import GeneralizedNoSQLAgent
from src.agent.drive_agent_node import drive_agent_node
from src.utils.logger import initialize_async_logging, cleanup_async_logging


# Initialize database agents with correct paths
db_path = os.path.join("/Users/yash/Documents/langgraph_as/src/agent/sales.db")
sql_agent = SQLAgent(db_path)
nosql_agent = GeneralizedNoSQLAgent(database_name="user_management_db")

# Initialize async components
async def initialize():
    """Initialize async components."""
    await initialize_async_logging()
    await nosql_agent.initialize()

# Initialize the NoSQL agent and async logging
asyncio.run(initialize())


async def router_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """Router node: Determines which agent should handle the next task.
    
    1. Get the next pending task
    2. Set the current task to be executed by the appropriate agent
    3. Return the task with the agent type for routing
    """
    try:
        # Get the next pending task
        current_task = next(
            (task for task in state.tasks if task.status == TaskStatus.PENDING),
            None
        )
        
        if not current_task:
            return {
                "tasks": state.tasks,
                "current_task": None,
                "results": state.results,
                "context": state.context
            }
        
        # Set the current task to be executed
        return {
            "tasks": state.tasks,
            "current_task": current_task,
            "results": state.results,
            "context": state.context
        }
            
    except Exception as e:
        logger.error(f"Error in router node: {str(e)}", exc_info=True)
        return {
            "tasks": state.tasks,
            "current_task": state.current_task,
            "results": state.results,
            "context": state.context,
            "errors": [f"Router error: {str(e)}"]
        }


async def sql_agent_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """SQL Agent node: Executes SQL tasks."""
    try:
        current_task = state.current_task
        if not current_task:
            return state.dict()
        
        logger.info(f"Executing SQL task with prompt: {current_task.query}")
        result = await sql_agent.execute_query(current_task.query)
        
        # Update task status and result
        current_task.status = TaskStatus.COMPLETED
        current_task.result = result
        
        # Update the tasks list with the completed task
        updated_tasks = [
            current_task if task.task_id == current_task.task_id else task
            for task in state.tasks
        ]
        
        return {
            "tasks": updated_tasks,
            "current_task": None,  # Reset current task
            "results": {
                **state.results,
                "completed_tasks": {
                    **state.results.get("completed_tasks", {}),
                    current_task.task_id: result
                }
            },
            "context": state.context
        }
            
    except Exception as e:
        logger.error(f"Error in SQL agent node: {str(e)}", exc_info=True)
        return {
            "tasks": state.tasks,
            "current_task": None,  # Reset current task on error
            "results": state.results,
            "context": state.context,
            "errors": [f"SQL agent error: {str(e)}"]
        }


async def nosql_agent_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """NoSQL Agent node: Executes NoSQL tasks."""
    try:
        current_task = state.current_task
        if not current_task:
            return state.dict()
        
        logger.info(f"Executing NoSQL task with prompt: {current_task.query}")
        result = await nosql_agent.execute_query(current_task.query)
        
        # Update task status and result
        current_task.status = TaskStatus.COMPLETED
        current_task.result = result
        
        # Update the tasks list with the completed task
        updated_tasks = [
            current_task if task.task_id == current_task.task_id else task
            for task in state.tasks
        ]
        
        return {
            "tasks": updated_tasks,
            "current_task": None,  # Reset current task
            "results": {
                **state.results,
                "completed_tasks": {
                    **state.results.get("completed_tasks", {}),
                    current_task.task_id: result
                }
            },
            "context": state.context
        }
            
    except Exception as e:
        logger.error(f"Error in NoSQL agent node: {str(e)}", exc_info=True)
        return {
            "tasks": state.tasks,
            "current_task": None,  # Reset current task on error
            "results": state.results,
            "context": state.context,
            "errors": [f"NoSQL agent error: {str(e)}"]
        }


def route_to_agent(state: State) -> str:
    """Routes tasks to the appropriate agent based on database type."""
    if not state.current_task:
        return END
    
    if state.current_task.database_type == DatabaseType.SQL:
        return "sql_agent"
    elif state.current_task.database_type == DatabaseType.NOSQL:
        return "nosql_agent"
    elif state.current_task.database_type == DatabaseType.GOOGLE_DRIVE:
        return "drive_agent"
    else:
        return END


def should_continue(state: State) -> Union[str, bool]:
    """Determines if the graph should continue processing."""
    # Check if there are any pending tasks
    has_pending_tasks = any(task.status == TaskStatus.PENDING for task in state.tasks)
    return "router" if has_pending_tasks else END


# Define the graph
graph = (
    StateGraph(State, config_schema=Configuration)
    .add_node("supervisor", supervisor_node)
    .add_node("router", router_node)
    .add_node("sql_agent", sql_agent_node)
    .add_node("nosql_agent", nosql_agent_node)
    .add_node("drive_agent", drive_agent_node)
    .add_edge("__start__", "supervisor")
    .add_conditional_edges(
        "supervisor",
        should_continue,
        {
            "router": "router",
            END: END
        }
    )
    .add_conditional_edges(
        "router",
        route_to_agent,
        {
            "sql_agent": "sql_agent",
            "nosql_agent": "nosql_agent",
            "drive_agent": "drive_agent",
            END: END
        }
    )
    .add_conditional_edges(
        "sql_agent",
        should_continue,
        {
            "router": "router",
            END: END
        }
    )
    .add_conditional_edges(
        "nosql_agent",
        should_continue,
        {
            "router": "router",
            END: END
        }
    )
    .add_conditional_edges(
        "drive_agent",
        should_continue,
        {
            "router": "router",
            END: END
        }
    )
    .compile(name="Multi-DB Query Orchestrator")
)