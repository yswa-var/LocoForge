import os
import streamlit as st
import logging
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage

# Load environment variables from .env file
load_dotenv()

from src.models.drive_models import DriveState
from src.graphs.drive_graph import create_drive_graph
from google_drive_ops import DriveAgent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set page config
st.set_page_config(
    page_title="Google Drive Assistant",
    page_icon="📁",
    layout="wide"
)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "drive_agent" not in st.session_state:
    st.session_state.drive_agent = DriveAgent(
        auth_method='oauth',
        credentials_path='credentials.json',
        token_path='token.json'
    )

if "drive_graph" not in st.session_state:
    st.session_state.drive_graph = create_drive_graph()

def process_drive_command(command: str) -> str:
    """Process a drive command using the LangGraph workflow."""
    try:
        logger.info(f"Processing command: {command}")
        
        # Create initial state
        state = DriveState(
            messages=[HumanMessage(content=command)],
            drive_agent=st.session_state.drive_agent,
            current_operation="",
            operation_result="",
            parameters={}  # Initialize empty parameters
        )
        
        # Run the graph
        result = st.session_state.drive_graph.invoke(state)
        
        return result["operation_result"]
    except Exception as e:
        logger.error(f"Error processing command: {str(e)}", exc_info=True)
        return f"❌ Error: {str(e)}"

# Set up the Streamlit page
st.title("Google Drive Assistant")
st.write("Chat with me to interact with your Google Drive!")

# Display chat messages from history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("What would you like to do with Google Drive?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        response = process_drive_command(prompt)
        st.markdown(response)
    
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": response})

# Add a sidebar with help information
with st.sidebar:
    st.header("Available Commands")
    st.markdown("""
    You can ask me to:
    - List files: "Show me all files in my Drive"
    - Download files: "Download file with ID: 123456789"
    - Upload files: "Upload file from: /path/to/file.txt"
    - Create folders: "Create a folder named: MyFolder"
    - Update files: "Update file with ID: 123456789"
    - Delete files: "Delete file with ID: 123456789"
    - Search files: "Search for files containing 'report'"
    """)
    
    # Add a button to clear chat history
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()