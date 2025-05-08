"""Define the multi-agent system graph."""

from typing import Any, Dict

from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph
from langgraph.prebuilt import ToolNode

from agent.configuration import Configuration
from agent.state import State
from agent.nodes import (
    supervisor_node,
    sql_agent_node,
    nosql_agent_node,
    drive_agent_node,
)

# Define the graph
workflow = StateGraph(State, config_schema=Configuration)

# Add nodes to the graph
workflow.add_node("supervisor", supervisor_node)
workflow.add_node("sql_agent", sql_agent_node)
workflow.add_node("nosql_agent", nosql_agent_node)
workflow.add_node("drive_agent", drive_agent_node)

# Define edges
workflow.add_edge("__start__", "supervisor")

# Add conditional edges from supervisor to specific agents
workflow.add_conditional_edges(
    "supervisor",
    lambda x: x["current_agent"],
    {
        "sql_agent": "sql_agent",
        "nosql_agent": "nosql_agent",
        "drive_agent": "drive_agent",
    }
)

# Add edges back to supervisor
workflow.add_edge("sql_agent", "supervisor")
workflow.add_edge("nosql_agent", "supervisor")
workflow.add_edge("drive_agent", "supervisor")

# Set the entrypoint
workflow.set_entry_point("supervisor")

# Compile the workflow
graph = workflow.compile()
graph.name = "Multi-Agent Database System"

# Add chat support
graph = graph.with_config({"recursion_limit": 25})
