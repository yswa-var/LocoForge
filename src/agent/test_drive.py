"""Test script for Drive Agent functionality."""

import asyncio
import sys
from pathlib import Path
from loguru import logger
from src.agent.google_drive_ops import DriveAgent

# Add the project root directory to Python path
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)


async def test_drive_agent():
    """Test basic Drive Agent operations."""
    try:
        # Hard-coded credentials and token path
        credentials_path = "/Users/yash/Documents/query_ochastrator/credentials.json"
        token_path = "/Users/yash/Documents/query_ochastrator/token.json"
        
        # Initialize the Drive Agent with OAuth
        logger.info("Initializing Drive Agent...")
        drive_agent = DriveAgent(
            auth_method='oauth',
            credentials_path=credentials_path,
            token_path=token_path
        )
        
        # Test listing files
        logger.info("Testing list files operation...")
        result = await drive_agent.execute_command("list files")
        logger.info(f"List files result: {result}")
        
        # Test creating a test folder
        logger.info("Testing create folder operation...")
        result = await drive_agent.execute_command("create folder named 'test_folder'")
        logger.info(f"Create folder result: {result}")
        
        # Clean up
        await drive_agent.close()
        logger.info("Test completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during test: {str(e)}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(test_drive_agent()) 