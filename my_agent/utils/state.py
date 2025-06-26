from typing import List, TypedDict
from langchain_core.messages import BaseMessage

class ChatState(TypedDict):
    """State for the chat application."""
    messages: List[BaseMessage] 