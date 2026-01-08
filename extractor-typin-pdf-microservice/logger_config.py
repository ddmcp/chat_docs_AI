import logging
import os
import sys
from logging.handlers import RotatingFileHandler

def setup_logger(name: str):
    """Configures a logger with both console and rotating file handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    # Logs format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 1. Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. File Handler
    log_dir = "/app/logs"
    if not os.path.exists(log_dir):
        # Fallback for local testing if not in container
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
    log_file = os.path.join(log_dir, "pdf-typing-microservice.log")
    
    try:
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"Could not initialize file logging: {e}")

    return logger
