from typing import Dict, Any, Annotated
import json
from langgraph.graph import StateGraph
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
import os

from src.models.drive_models import DriveState, DriveOperation
from src.prompts.drive_prompts import create_drive_prompt
from src.utils.formatting import (
    format_file_list,
    format_download_response,
    format_folder_response,
    format_delete_response,
    format_search_response
)

def create_drive_graph() -> StateGraph:
    """Create the Google Drive workflow graph."""
    
    # Create the graph
    workflow = StateGraph(DriveState)
    
    # Define the nodes
    def parse_command(state: DriveState) -> DriveState:
        """Parse the user's command into a drive operation."""
        # Create the chain
        prompt = create_drive_prompt()
        
        # Get OpenAI API key from environment variable
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise ValueError(
                "OpenAI API key not found. Please set the OPENAI_API_KEY environment variable. "
                "You can get your API key from https://platform.openai.com/api-keys"
            )
        
        llm = ChatOpenAI(
            model="gpt-3.5-turbo",
            temperature=0,
            api_key=openai_api_key
        )
        parser = JsonOutputParser(pydantic_object=DriveOperation)
        chain = prompt | llm | parser
        
        # Get the last message
        last_message = state["messages"][-1]
        
        # Parse the command
        operation_dict = chain.invoke({"input": last_message.content})
        
        # Update state
        state["current_operation"] = operation_dict["operation"]
        state["parameters"] = operation_dict["parameters"]
        return state
    
    def execute_operation(state: DriveState) -> DriveState:
        """Execute the drive operation."""
        drive_agent = state["drive_agent"]
        operation = state["current_operation"]
        parameters = state["parameters"]
        
        # Format the command based on the operation type
        if operation == "list":
            command = "list files"
        elif operation == "download":
            command = f"download file id: {parameters.get('file_id', '')}"
        elif operation == "upload":
            command = f"upload file from: {parameters.get('file_path', '')}"
        elif operation == "create_folder":
            command = f"create folder named: {parameters.get('name', '')}"
        elif operation == "update":
            command = f"update file id: {parameters.get('file_id', '')}"
        elif operation == "delete":
            command = f"delete file id: {parameters.get('file_id', '')}"
        elif operation == "search":
            command = f"search for: {parameters.get('query_text', '')}"
        else:
            command = f"{operation} {' '.join(f'{k}: {v}' for k, v in parameters.items())}"
        
        # Execute the operation
        result = drive_agent.execute_command(command)
        
        # Format the response based on the operation type
        if operation == "list":
            try:
                result_dict = json.loads(result)
                if 'files' in result_dict:
                    formatted_result = format_file_list(result_dict['files'])
            except json.JSONDecodeError as e:
                formatted_result = f"❌ Error: Failed to parse response: {str(e)}"
        elif operation == "download":
            formatted_result = format_download_response(result)
        elif operation == "create_folder":
            formatted_result = format_folder_response(result)
        elif operation == "delete":
            formatted_result = format_delete_response(result)
        elif operation == "search":
            formatted_result = format_search_response(result)
        else:
            formatted_result = result
        
        # Update state
        state["operation_result"] = formatted_result
        return state
    
    # Add nodes to the graph
    workflow.add_node("parse_command", parse_command)
    workflow.add_node("execute_operation", execute_operation)
    
    # Add edges
    workflow.add_edge("parse_command", "execute_operation")
    workflow.set_entry_point("parse_command")
    
    # Compile the graph
    return workflow.compile() 