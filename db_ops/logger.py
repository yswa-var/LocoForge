"""Logging configuration module."""

import logging
import os
from datetime import datetime
import sys
from typing import Optional

# Create logs directory if it doesn't exist
logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(logs_dir, exist_ok=True)

def setup_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Set up and configure a logger.
    
    Args:
        name: Name of the logger
        level: Logging level (defaults to INFO)
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Set level if provided, otherwise use INFO
    logger.setLevel(level or logging.INFO)
    
    # Create console handler if no handlers exist
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level or logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
    
    return logger

# Create a default logger instance
default_logger = setup_logger('locoforge')

# Export both the setup function and the default logger
__all__ = ['setup_logger', 'default_logger'] 