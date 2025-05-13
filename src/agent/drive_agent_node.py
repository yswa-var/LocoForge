"""Drive Agent node implementation for Google Drive operations."""

from typing import Dict, Any
from loguru import logger
from langchain_core.runnables import RunnableConfig

from src.agent.types import State, TaskStatus
from src.agent.google_drive_ops import DriveAgent

# Hard-coded credentials and token path
CREDENTIALS_PATH = "/Users/yash/Documents/query_ochastrator/credentials.json"
TOKEN_PATH = "/Users/yash/Documents/query_ochastrator/token.json"

# Initialize the Drive Agent with OAuth
drive_agent = DriveAgent(
    auth_method='oauth',
    credentials_path=CREDENTIALS_PATH,
    token_path=TOKEN_PATH
)


async def drive_agent_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """Drive Agent node: Executes Google Drive tasks."""
    try:
        current_task = state.current_task
        if not current_task:
            return state.dict()
        
        logger.info(f"Executing Drive task with prompt: {current_task.query}")
        result = await drive_agent.execute_command(current_task.query)
        
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
        logger.error(f"Error in Drive agent node: {str(e)}", exc_info=True)
        return {
            "tasks": state.tasks,
            "current_task": None,  # Reset current task on error
            "results": state.results,
            "context": state.context,
            "errors": [f"Drive agent error: {str(e)}"]
        }


# Export the drive agent node for use in the graph
__all__ = ["drive_agent_node"] 