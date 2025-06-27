# Basic LangGraph Chat Application

A simple chat application built with LangGraph and OpenAI, following the LangGraph Platform deployment structure.

## File Structure

```
lgstudioSetup/
├── my_agent/                    # All project code
│   ├── utils/                   # Utilities for your graph
│   │   ├── __init__.py
│   │   ├── nodes.py            # Node functions for your graph
│   │   └── state.py            # State definition of your graph
│   ├── requirements.txt        # Package dependencies
│   ├── __init__.py
│   └── agent.py               # Code for constructing your graph
├── .env                        # Environment variables
├── langgraph.json             # Configuration file for LangGraph
├── test_chat.py               # Test script
└── README.md                  # This file
```

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r my_agent/requirements.txt
   ```

2. **Set up environment variables:**
   Edit the `.env` file and add your OpenAI API key:
   ```
   OPENAI_API_KEY=your_actual_openai_api_key_here
   ```

## Usage

### Test the application:
```bash
python test_chat.py
```

### Use in your own code:
```python
from langchain_core.messages import HumanMessage
from my_agent.agent import graph
from my_agent.utils.state import ChatState

# Create initial state
initial_state = ChatState(
    messages=[HumanMessage(content="Hello!")]
)

# Run the graph
result = graph.invoke(initial_state)

# Access the conversation
for message in result["messages"]:
    print(f"{message.type}: {message.content}")
```

## How it works

This is a simple chat application with a single node:

1. **State**: `ChatState` contains a list of messages
2. **Node**: `chat_node` processes the latest message using OpenAI's GPT-3.5-turbo
3. **Graph**: Simple linear flow: START → chat → END

The graph takes user messages, sends them to OpenAI, and returns the AI's response.

## Deployment

This application is configured for LangGraph Platform deployment using the `langgraph.json` configuration file. The structure follows the recommended patterns for LangGraph applications. # LocoForge2
