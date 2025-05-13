"""Supervisor node implementation for query analysis and task creation."""

from typing import Dict, Any, List
from loguru import logger
from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
import json
import asyncio

from src.agent.types import State, Task, DatabaseType, TaskStatus


async def supervisor_node(state: State, config: RunnableConfig) -> Dict[str, Any]:
    """Supervisor node that analyzes queries and creates simple tasks."""
    try:
        # Initialize LLM
        llm = ChatOpenAI(
            model=config.get("model_name", "gpt-3.5-turbo"),
            temperature=config.get("temperature", 0.7)
        )

        # Create simple system prompt
        system_prompt = """You are a query routing system. Your job is to:
1. Analyze if the query needs SQL, NoSQL, or Google Drive operations
2. Create a simple task with the natural language query
3. No need to generate SQL or process the query - just pass it through

Respond with a JSON object in this format:
{
    "tasks": [
        {
            "task_id": "task_1",
            "database_type": "sql" or "nosql" or "google_drive",
            "query": "the original natural language query",
            "priority": 1
        }
    ]
}

For Google Drive operations, look for queries about:
- Listing files or folders
- Uploading files
- Downloading files
- Creating folders
- Searching files
- Updating file metadata
- Deleting files"""

        # Create messages for LLM
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=state.user_query)
        ]

        # Get LLM response - move to thread to avoid blocking
        logger.info("Analyzing query with LLM")
        response = await asyncio.to_thread(llm.invoke, messages)
        task_analysis = json.loads(response.content)

        # Convert task analysis to Task objects
        tasks = []
        for task_data in task_analysis["tasks"]:
            task = Task(
                task_id=task_data["task_id"],
                database_type=DatabaseType(task_data["database_type"]),
                query=task_data["query"],
                priority=task_data["priority"],
                status=TaskStatus.PENDING
            )
            tasks.append(task)

        # Update state
        return {
            "tasks": tasks,
            "current_task": tasks[0] if tasks else None,
            "context": {},
            "errors": []
        }

    except Exception as e:
        logger.error(f"Error in supervisor node: {str(e)}", exc_info=True)
        return {
            "tasks": [],
            "current_task": None,
            "context": {},
            "errors": [f"Failed to analyze query: {str(e)}"]
        }


# Export the supervisor node for use in the graph
__all__ = ["supervisor_node"] 