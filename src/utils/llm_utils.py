"""Utility functions for LLM message processing."""

from typing import Dict, Any, List
import json
from src.utils.llm_config import llm
import re

async def process_message(messages: List[Dict[str, str]], system_prompt: str = None) -> Dict[str, Any]:
    """
    Process a message using the LLM.
    
    Args:
        messages: List of message dictionaries with 'role' and 'content'
        system_prompt: Optional system prompt to guide the LLM
        
    Returns:
        Dict containing the processed response with 'function' and 'arguments' keys
    """
    try:
        # Add system prompt if provided
        if system_prompt:
            messages.insert(0, {"role": "system", "content": system_prompt})
        
        # Get response from LLM asynchronously
        response = await llm.ainvoke(messages)
        
        # Parse the response content
        content = response.content
        
        # Try to extract function name and arguments
        try:
            # Extract function name
            function_match = re.search(r"FUNCTION:\s*(\w+)", content)
            if not function_match:
                raise ValueError("Could not find function name in response")
            function_name = function_match.group(1)
            
            # Extract arguments
            args_match = re.search(r"ARGUMENTS:\s*(\{.*\})", content, re.DOTALL)
            if not args_match:
                raise ValueError("Could not find arguments in response")
            arguments = json.loads(args_match.group(1))
            
            return {
                "function": function_name,
                "arguments": arguments
            }
        except (json.JSONDecodeError, re.error) as e:
            return {
                "error": f"Failed to parse LLM response: {str(e)}",
                "status": "error"
            }
            
    except Exception as e:
        return {
            "error": f"Failed to process message: {str(e)}",
            "status": "error"
        } 