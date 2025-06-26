from typing import Dict, Any
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage
from my_agent.utils.state import ChatState

def chat_node(state: ChatState) -> ChatState:
    """
    Simple chat node that processes the latest message and generates a response.
    """
    # Get the latest message (assuming it's from the user)
    messages = state["messages"]
    
    # Create OpenAI chat model
    model = ChatOpenAI(model="gpt-3.5-turbo")
    
    # Generate response
    response = model.invoke(messages)
    
    # Add the AI response to the messages
    messages.append(response)
    
    return {"messages": messages} 