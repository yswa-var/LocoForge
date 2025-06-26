from langgraph.graph import StateGraph, END, START
from my_agent.utils.nodes import chat_node
from my_agent.utils.state import ChatState

# Create the workflow
workflow = StateGraph(ChatState)

# Add the single chat node
workflow.add_node("chat", chat_node)

# Add edges: START -> chat -> END
workflow.add_edge(START, "chat")
workflow.add_edge("chat", END)

# Compile the graph
graph = workflow.compile() 