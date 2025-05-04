import streamlit as st
from main import create_graph, GraphState
from langchain_core.messages import HumanMessage, AIMessage
import pandas as pd

# Set page config
st.set_page_config(
    page_title="LangGraph Chat",
    page_icon="🤖",
    layout="wide"  # Changed to wide layout to accommodate sidebar
)

# Initialize the graph
@st.cache_resource
def get_graph():
    return create_graph()

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Sidebar for chat history
with st.sidebar:
    st.title("💬 Chat History")
    
    # Add a clear chat button
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()
    
    # Display chat history in reverse order (newest first)
    for message in reversed(st.session_state.messages):
        with st.expander(f"{message['role'].title()}: {message['content'][:50]}..." if len(message['content']) > 50 else f"{message['role'].title()}: {message['content']}"):
            st.markdown(message['content'])
            if "results" in message:
                st.write("Results:")
                st.json(message["results"])

# Main content area
st.title("🤖 LangGraph Chat")

# Display chat messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("What's on your mind?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Get the graph
    graph = get_graph()
    
    # Create initial state with the new message
    initial_state = {
        "messages": [HumanMessage(content=prompt)]
    }
    
    # Process the message through the graph
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            # Run the graph
            result = graph.invoke(initial_state)
            
            # Get the last message (the response)
            response = result["messages"][-1].content
            
            # Display the response
            st.markdown(response)
            
            # Display results as tables if available
            if "results" in result and result["results"]:
                for agent_type, result_data in result["results"].items():
                    st.subheader(f"{agent_type.upper()} Results")
                    
                    # Check if the result is already in our structured format
                    if isinstance(result_data, dict) and "data" in result_data:
                        # Display the structured data
                        st.json(result_data)
                        
                        # Optionally display as a table as well
                        if result_data.get("data"):
                            df = pd.DataFrame(result_data["data"])
                            st.dataframe(df)
                    else:
                        # Handle legacy or unstructured data
                        try:
                            if isinstance(result_data, dict):
                                st.json(result_data)
                            elif isinstance(result_data, pd.DataFrame):
                                st.dataframe(result_data)
                            elif isinstance(result_data, list):
                                st.json(result_data)
                            else:
                                st.write(result_data)
                        except Exception as e:
                            st.error(f"Error displaying results: {str(e)}")
                            st.write("Raw result:", result_data)
            
            # Add assistant response to chat history with results
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response,
                "results": result.get("results", {})
            }) 