#!/usr/bin/env python3
"""
Simple Chat Function for Testing Gemini Agents
A straightforward way to test agents with Gemini 2.0 Flash Lite
"""

import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, AIMessage
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

class SimpleGeminiChat:
    """Simple chat interface for testing Gemini agents"""
    
    def __init__(self, model_name: str = "gemini-2.0-flash-lite"):
        """
        Initialize the chat with Gemini model
        
        Args:
            model_name: The Gemini model to use (default: gemini-2.0-flash-lite)
        """
        self.model_name = model_name
        self.model = ChatGoogleGenerativeAI(
            model=model_name,
            google_api_key=os.getenv("GEMINI_KEY")
        )
        self.conversation_history: List[Dict[str, Any]] = []
        
    def chat(self, message: str, system_prompt: str = None) -> str:
        """
        Send a message and get a response
        
        Args:
            message: The user's message
            system_prompt: Optional system prompt to set context
            
        Returns:
            The AI's response
        """
        # Prepare messages
        messages = []
        
        # Add system prompt if provided
        if system_prompt:
            messages.append(HumanMessage(content=f"System: {system_prompt}"))
        
        # Add conversation history
        for entry in self.conversation_history:
            if entry["role"] == "user":
                messages.append(HumanMessage(content=entry["content"]))
            else:
                messages.append(AIMessage(content=entry["content"]))
        
        # Add current message
        messages.append(HumanMessage(content=message))
        
        # Get response
        response = self.model.invoke(messages)
        
        # Store in conversation history
        self.conversation_history.append({"role": "user", "content": message})
        self.conversation_history.append({"role": "assistant", "content": response.content})
        
        return response.content
    
    def clear_history(self):
        """Clear the conversation history"""
        self.conversation_history = []
    
    def get_history(self) -> List[Dict[str, Any]]:
        """Get the conversation history"""
        return self.conversation_history.copy()

def create_agent(agent_type: str = "general") -> SimpleGeminiChat:
    """
    Create a pre-configured agent
    
    Args:
        agent_type: Type of agent to create
            - "general": General purpose assistant
            - "coder": Programming assistant
            - "writer": Writing assistant
            - "analyst": Data analysis assistant
            - "creative": Creative writing assistant
    
    Returns:
        Configured SimpleGeminiChat instance
    """
    system_prompts = {
        "general": "You are a helpful AI assistant. Provide clear, accurate, and helpful responses.",
        "coder": "You are a programming assistant. Write clean, efficient, and well-documented code. Explain your solutions clearly.",
        "writer": "You are a writing assistant. Help with writing, editing, and creative content. Provide constructive feedback.",
        "analyst": "You are a data analysis assistant. Help with data interpretation, statistics, and analytical thinking.",
        "creative": "You are a creative writing assistant. Help with storytelling, poetry, and creative content generation."
    }
    
    agent = SimpleGeminiChat()
    agent.system_prompt = system_prompts.get(agent_type, system_prompts["general"])
    return agent

def interactive_chat():
    """Run an interactive chat session"""
    print("🤖 Simple Gemini Chat")
    print("=" * 50)
    
    # Choose agent type
    print("\nChoose an agent type:")
    print("1. General Assistant")
    print("2. Coder")
    print("3. Writer")
    print("4. Analyst")
    print("5. Creative")
    
    choice = input("\nEnter your choice (1-5) or press Enter for General: ").strip()
    
    agent_types = {
        "1": "general",
        "2": "coder", 
        "3": "writer",
        "4": "analyst",
        "5": "creative"
    }
    
    agent_type = agent_types.get(choice, "general")
    agent = create_agent(agent_type)
    
    print(f"\n🎯 Created {agent_type.title()} Agent")
    print("Type 'quit' to exit, 'clear' to clear history, 'history' to see conversation")
    print("-" * 50)
    
    while True:
        try:
            user_input = input("\nYou: ").strip()
            
            if user_input.lower() == 'quit':
                print("👋 Goodbye!")
                break
            elif user_input.lower() == 'clear':
                agent.clear_history()
                print("🗑️  Conversation history cleared")
                continue
            elif user_input.lower() == 'history':
                history = agent.get_history()
                if not history:
                    print("📝 No conversation history")
                else:
                    print("\n📝 Conversation History:")
                    for i, entry in enumerate(history, 1):
                        role = "You" if entry["role"] == "user" else "AI"
                        print(f"{i}. {role}: {entry['content'][:100]}...")
                continue
            elif not user_input:
                continue
            
            # Get response
            response = agent.chat(user_input, agent.system_prompt)
            print(f"\n🤖 AI: {response}")
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    # Check if GEMINI_KEY is available
    if not os.getenv("GEMINI_KEY"):
        print("❌ Error: GEMINI_KEY not found in environment variables")
        print("Please make sure your .env file contains: GEMINI_KEY=your_api_key_here")
        exit(1)
    
    interactive_chat() 