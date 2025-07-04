#!/usr/bin/env python3
"""
Test script for the basic chat application.
Make sure to set your OPENAPI_KEY in the .env file before running.
"""

import os
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from my_agent.agent import graph
from my_agent.utils.state import ChatState

# Load environment variables
load_dotenv()

def test_chat():
    """Test the chat application with a database schema query."""
    
    # Check if API key is set
    if not os.getenv("OPENAPI_KEY"):
        print("Please set your OPENAPI_KEY in the .env file")
        return
    
    # Create initial state with a database schema query
    initial_state = ChatState(
        messages=[
            HumanMessage(content="what the sql database schema?")
        ]
    )
    
    # Run the graph
    print("Sending message to chat...")
    result = graph.invoke(initial_state)
    
    # Print the conversation
    print("\nConversation:")
    for i, message in enumerate(result["messages"]):
        role = "User" if isinstance(message, HumanMessage) else "Assistant"
        print(f"{role}: {message.content}")
        print()

# Test the chat functionality
if __name__ == "__main__":
    test_chat() 