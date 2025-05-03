from typing import Dict, List, TypedDict, Annotated, Sequence, Any
from pydantic import BaseModel, Field
from langchain_core.messages import HumanMessage, AIMessage

class DriveState(TypedDict):
    """State for the Google Drive workflow."""
    messages: Annotated[Sequence[HumanMessage | AIMessage], "The messages in the conversation"]
    drive_agent: Annotated[Any, "The Google Drive agent"]
    current_operation: Annotated[str, "The current operation being performed"]
    operation_result: Annotated[str, "The result of the operation"]
    parameters: Annotated[Dict[str, str], "Parameters for the current operation"]

class DriveOperation(BaseModel):
    """Schema for drive operations."""
    operation: str = Field(description="The operation to perform (list, download, upload, create_folder, update, delete, search)")
    parameters: Dict[str, str] = Field(description="Parameters for the operation") 