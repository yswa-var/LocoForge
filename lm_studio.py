import requests
import logging
from typing import Dict, List, Optional
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LMStudioClient:
    """Client for interacting with LM Studio's local API."""
    
    def __init__(self, base_url: str = "http://localhost:1234"):
        self.base_url = base_url
        self.chat_endpoint = f"{base_url}/v1/chat/completions"
        logger.info(f"Initialized LMStudioClient with base URL: {base_url}")
    
    def chat_completion(
        self,
        messages: List[BaseMessage],
        model: str = "gemma-3-4b-it",
        temperature: float = 0.7,
        max_tokens: int = -1,
        stream: bool = False
    ) -> Dict:
        """
        Send a chat completion request to LM Studio.
        
        Args:
            messages: List of messages in the conversation
            model: The model to use
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate (-1 for unlimited)
            stream: Whether to stream the response
            
        Returns:
            Dict containing the API response
        """
        try:
            # Convert langchain messages to LM Studio format
            formatted_messages = []
            for msg in messages:
                if isinstance(msg, HumanMessage):
                    role = "user"
                elif isinstance(msg, AIMessage):
                    role = "assistant"
                else:
                    role = "system"
                formatted_messages.append({
                    "role": role,
                    "content": msg.content
                })
            
            payload = {
                "model": model,
                "messages": formatted_messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "stream": stream
            }
            
            logger.info(f"Sending request to LM Studio with model: {model}")
            logger.debug(f"Request payload: {payload}")
            
            response = requests.post(
                self.chat_endpoint,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code != 200:
                error_msg = f"LM Studio API error: {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            result = response.json()
            logger.info("Successfully received response from LM Studio")
            logger.debug(f"Response: {result}")
            return result
            
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Failed to connect to LM Studio at {self.base_url}. Is it running?"
            logger.error(error_msg)
            raise Exception(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error while communicating with LM Studio: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
