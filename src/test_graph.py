"""Test script for running the multi-agent graph in LangGraph Studio."""

from agent.graph import graph
from agent.state import State
from langchain_core.messages import HumanMessage, AIMessage

async def test_graph():
    # Initialize state with chat messages
    initial_state = State(
        messages=[
            HumanMessage(content="Show me all users in the database").dict()
        ]
    )
    
    # Run the graph
    result = await graph.ainvoke(initial_state)
    print("Graph Result:", result)

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_graph()) 