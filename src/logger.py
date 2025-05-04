import logging
import os
from datetime import datetime

# Create logs directory if it doesn't exist
logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Configure logging
def setup_logger(name):
    logger = logging.getLogger(name)
    
    # Prevent adding handlers multiple times
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Create formatters
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_formatter = logging.Formatter(
            '%(levelname)s: %(message)s'
        )

        # Create and configure file handler
        log_file = os.path.join(logs_dir, f'{datetime.now().strftime("%Y-%m-%d")}.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(file_formatter)

        # Create and configure console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

# Create a default logger instance
default_logger = setup_logger('locoforge')

# Export both the setup function and the default logger
__all__ = ['setup_logger', 'default_logger'] 