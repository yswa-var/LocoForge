#!/usr/bin/env python3
"""
Test Script for Simple Gemini Agents
Demonstrates how to use the SimpleGeminiChat class programmatically
"""

from my_agent.utils.simple_chat import SimpleGeminiChat, create_agent

def test_basic_chat():
    """Test basic chat functionality"""
    print("🧪 Testing Basic Chat")
    print("-" * 30)
    
    chat = SimpleGeminiChat()
    
    # Simple test
    response = chat.chat("Hello! What's 2 + 2?")
    print(f"Q: Hello! What's 2 + 2?")
    print(f"A: {response}")
    print()

def test_coder_agent():
    """Test the coder agent"""
    print("🧪 Testing Coder Agent")
    print("-" * 30)
    
    coder = create_agent("coder")
    
    # Test coding question
    response = coder.chat("Write a Python function to calculate fibonacci numbers")
    print(f"Q: Write a Python function to calculate fibonacci numbers")
    print(f"A: {response}")
    print()

def test_writer_agent():
    """Test the writer agent"""
    print("🧪 Testing Writer Agent")
    print("-" * 30)
    
    writer = create_agent("writer")
    
    # Test writing question
    response = writer.chat("Help me write a short story about a robot learning to paint")
    print(f"Q: Help me write a short story about a robot learning to paint")
    print(f"A: {response}")
    print()

def test_conversation_history():
    """Test conversation history functionality"""
    print("🧪 Testing Conversation History")
    print("-" * 30)
    
    chat = SimpleGeminiChat()
    
    # First message
    response1 = chat.chat("My name is Alice")
    print(f"Q: My name is Alice")
    print(f"A: {response1}")
    
    # Second message (should remember the name)
    response2 = chat.chat("What's my name?")
    print(f"Q: What's my name?")
    print(f"A: {response2}")
    
    # Show history
    history = chat.get_history()
    print(f"\n📝 Conversation has {len(history)} messages")
    print()

def test_custom_system_prompt():
    """Test with custom system prompt"""
    print("🧪 Testing Custom System Prompt")
    print("-" * 30)
    
    chat = SimpleGeminiChat()
    
    custom_prompt = "You are a helpful math tutor. Always explain your reasoning step by step."
    response = chat.chat("Solve: 3x + 7 = 22", system_prompt=custom_prompt)
    print(f"Q: Solve: 3x + 7 = 22")
    print(f"A: {response}")
    print()

def run_all_tests():
    """Run all test functions"""
    print("🚀 Running All Tests")
    print("=" * 50)
    
    try:
        test_basic_chat()
        test_coder_agent()
        test_writer_agent()
        test_conversation_history()
        test_custom_system_prompt()
        
        print("✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")

if __name__ == "__main__":
    run_all_tests() 