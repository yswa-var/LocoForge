from llm_config import llm
from logger import default_logger as logger

def process_message(messages: list) -> str:
    """Process messages and return LLM response."""
    logger.debug("Processing LLM messages")
    try:
        response = llm.invoke(messages)
        return response.content
    except Exception as e:
        logger.error(f"Error processing LLM message: {str(e)}", exc_info=True)
        raise Exception(
            "Failed to process your request",
            "Please ensure your request is clear and try again. If the problem persists, contact support."
        ) 