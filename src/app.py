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
                    if isinstance(result_data, dict):
                        # Handle NoSQL results which are nested in a dictionary
                        if "results" in result_data:
                            actual_results = result_data["results"]
                            if isinstance(actual_results, list):
                                # Display each result in a pretty JSON format
                                for idx, res in enumerate(actual_results, 1):
                                    with st.expander(f"Result {idx}", expanded=True):
                                        st.json(res)
                            elif isinstance(actual_results, dict):
                                st.json(actual_results)
                            else:
                                st.write(actual_results)
                        else:
                            # If it's a regular dictionary without nested results
                            st.json(result_data)
                    elif isinstance(result_data, pd.DataFrame):
                        # Only use DataFrame display for actual tabular data
                        if len(result_data.columns) > 1:  # If it's a proper table
                            st.dataframe(result_data, use_container_width=True)
                        else:
                            st.json(result_data.to_dict(orient='records'))
                    elif isinstance(result_data, list):
                        # Display list items in JSON format
                        for idx, item in enumerate(result_data, 1):
                            with st.expander(f"Item {idx}", expanded=True):
                                st.json(item)
                    else:
                        st.write(result_data)
            
            # Add assistant response to chat history with results
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response,
                "results": result.get("results", {})
            }) 