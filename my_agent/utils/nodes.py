from typing import Dict, Any
import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, AIMessage
from my_agent.utils.state import ChatState

# Load environment variables
load_dotenv()

def chat_node(state: ChatState) -> ChatState:
    """
    Simple chat node that processes the latest message and generates a response using Gemini.
    """
    # Get the latest message (assuming it's from the user)
    messages = state["messages"]
    
    # Create Gemini chat model using the API key from .env
    model = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-lite",
        google_api_key=os.getenv("GEMINI_KEY")
    )
    
    # Generate response
    response = model.invoke(messages)
    
    # Add the AI response to the messages
    messages.append(response)
    
    return {"messages": messages} 