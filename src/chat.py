"""Chat interface for the multi-agent system."""

import asyncio
from typing import List, Dict, Any
from langchain_core.messages import HumanMessage, AIMessage
from agent.graph import graph
from agent.state import State

class MultiAgentChat:
    def __init__(self):
        self.state = State(messages=[])
    
    async def chat(self, user_input: str) -> Dict[str, Any]:
        """Process a user message and return the system's response."""
        # Create new state with updated messages
        new_messages = self.state.messages.copy()
        new_messages.append(HumanMessage(content=user_input).model_dump())
        new_state = State(messages=new_messages)
        
        # Run the graph
        result = await graph.ainvoke(new_state)
        
        # Format the response
        if hasattr(result, 'error') and result.error:
            response = f"Error: {result.error}"
        elif hasattr(result, 'sql_result') and result.sql_result:
            response = f"SQL Result: {result.sql_result}"
        elif hasattr(result, 'nosql_result') and result.nosql_result:
            response = f"NoSQL Result: {result.nosql_result}"
        else:
            response = "No result available"
            
        # Update state with new messages
        result.messages.append(AIMessage(content=response).model_dump())
        self.state = result
        
        return {
            "response": response,
            "current_agent": getattr(result, 'current_agent', None),
            "task_type": getattr(result, 'task_type', None)
        }

async def main():
    chat = MultiAgentChat()
    print("Multi-Agent Chat System")
    print("Type 'exit' to quit")
    print("-" * 50)
    
    while True:
        user_input = input("\nYou: ").strip()
        if user_input.lower() == 'exit':
            break
            
        try:
            result = await chat.chat(user_input)
            print(f"\nSystem: {result['response']}")
            if result['current_agent']:
                print(f"Handled by: {result['current_agent']}")
        except Exception as e:
            print(f"\nError: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main()) 