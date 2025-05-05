import streamlit as st
from main import create_graph, GraphState
from langchain_core.messages import HumanMessage, AIMessage
import pandas as pd
import json
from datetime import datetime
import asyncio
import nest_asyncio
# Set page config first
st.set_page_config(
    page_title="LocoForge - Multi-Agent Assistant",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# # Custom CSS for better styling
# st.markdown("""
# <style>
#     .main {
#         padding: 2rem;
#     }
#     .stChatMessage {
#         padding: 1rem;
#         border-radius: 0.5rem;
#         margin-bottom: 1rem;
#         border: 1px solid #e0e0e0;
#     }
#     .stChatMessage[data-testid="stChatMessage"] {
#         background-color: #f8f9fa;
#     }
#     .stChatMessage[data-testid="stChatMessage"] .user {
#         background-color: #e3f2fd;
#     }
#     .stChatMessage[data-testid="stChatMessage"] .assistant {
#         background-color: #f5f5f5;
#     }
#     .stButton button {
#         width: 100%;
#         border-radius: 0.5rem;
#         padding: 0.5rem 1rem;
#         background-color: #4CAF50;
#         color: white;
#     }
#     .stButton button:hover {
#         background-color: #45a049;
#     }
#     .result-container {
#         padding: 1rem;
#         border-radius: 0.5rem;
#         background-color: #f8f9fa;
#         margin: 1rem 0;
#     }
#     .agent-type {
#         font-size: 1.2rem;
#         font-weight: bold;
#         color: #2196F3;
#         margin-bottom: 0.5rem;
#     }
#     .timestamp {
#         font-size: 0.8rem;
#         color: #666;
#         margin-top: 0.5rem;
#     }
# </style>
# """, unsafe_allow_html=True)

# Initialize the graph
@st.cache_resource
def get_graph():
    return create_graph()

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "drive_agent" not in st.session_state:
    st.session_state.drive_agent = None

# Sidebar
with st.sidebar:
    st.title("🤖 LocoForge")
    st.markdown("---")
    

    
    # Chat History
    st.subheader("💬 Chat History")
    
    # Add a clear chat button with confirmation
    if st.button("🗑️ Clear Chat History", key="clear_chat"):
        if st.session_state.messages:
            st.session_state.messages = []
            st.rerun()
    
    # Display chat history in reverse order (newest first)
    for message in reversed(st.session_state.messages):
        with st.expander(
            f"{message['role'].title()}: {message['content'][:50]}..." if len(message['content']) > 50 
            else f"{message['role'].title()}: {message['content']}"
        ):
            st.markdown(message['content'])
            if "results" in message:
                st.markdown("### Results")
                st.json(message["results"])
            if "timestamp" in message:
                st.markdown(f"*{message['timestamp']}*")

# Main content area
st.title("🤖 LocoForge Assistant")
st.markdown("""
I can help you with:
- 📊 SQL database queries
- 📝 NoSQL database operations
- 📁 Google Drive file management
- 🔄 Combined operations across multiple systems
""")

# Display chat messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "results" in message:
            with st.expander("View Results", expanded=False):
                for agent_type, result_data in message["results"].items():
                    st.markdown(f"### {agent_type.upper()} Results")
                    if isinstance(result_data, dict) and "data" in result_data:
                        # Display structured data
                        st.json(result_data)
                        if result_data.get("data"):
                            df = pd.DataFrame(result_data["data"])
                            st.dataframe(df, use_container_width=True)
                    else:
                        try:
                            if isinstance(result_data, dict):
                                st.json(result_data)
                            elif isinstance(result_data, pd.DataFrame):
                                st.dataframe(result_data, use_container_width=True)
                            elif isinstance(result_data, list):
                                st.json(result_data)
                            else:
                                st.write(result_data)
                        except Exception as e:
                            st.error(f"Error displaying results: {str(e)}")
                            st.write("Raw result:", result_data)

# Process message through the graph
async def process_message(prompt: str, graph, initial_state: dict) -> dict:
    """Process a message through the graph asynchronously."""
    return await graph.ainvoke(initial_state)

# Chat input
if prompt := st.chat_input("What would you like me to help you with?"):
    # Add user message to chat history with timestamp
    st.session_state.messages.append({
        "role": "user",
        "content": prompt,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })
    
    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Get the graph
    graph = get_graph()
    
    # Create initial state with the new message
    initial_state = {
        "messages": [HumanMessage(content=prompt)],
        "task_spec": {},
        "results": {},
        "error": None,
        "drive_agent": st.session_state.drive_agent
    }
    
    # Process the message through the graph
    with st.chat_message("assistant"):
        with st.spinner("Processing your request..."):
            # Run the graph
            # result = asyncio.run(process_message(prompt, graph, initial_state))
            nest_asyncio.apply()
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            result = loop.run_until_complete(process_message(prompt, graph, initial_state))
            
            
            # Update drive_agent in session state
            if "drive_agent" in result:
                st.session_state.drive_agent = result["drive_agent"]
            
            # Get the last message (the response)
            response = result["messages"][-1].content
            
            # Display the response
            st.markdown(response)
            
            # Display results as tables if available
            if "results" in result and result["results"]:
                for agent_type, result_data in result["results"].items():
                    with st.expander(f"📊 {agent_type.upper()} Results", expanded=True):
                        # Check if the result is already in our structured format
                        if isinstance(result_data, dict) and "data" in result_data:
                            # Display the structured data
                            st.json(result_data)
                            
                            # Display as a table
                            if result_data.get("data"):
                                df = pd.DataFrame(result_data["data"])
                                st.dataframe(df, use_container_width=True)
                        else:
                            # Handle legacy or unstructured data
                            try:
                                if isinstance(result_data, dict):
                                    st.json(result_data)
                                elif isinstance(result_data, pd.DataFrame):
                                    st.dataframe(result_data, use_container_width=True)
                                elif isinstance(result_data, list):
                                    st.json(result_data)
                                else:
                                    st.write(result_data)
                            except Exception as e:
                                st.error(f"Error displaying results: {str(e)}")
                                st.write("Raw result:", result_data)
            
            # Add assistant response to chat history with results and timestamp
            st.session_state.messages.append({
                "role": "assistant",
                "content": response,
                "results": result.get("results", {}),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }) 