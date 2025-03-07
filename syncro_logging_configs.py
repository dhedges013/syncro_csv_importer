import os
import logging
from datetime import datetime

# Logging Configuration
LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "logs"))
os.makedirs(LOG_DIR, exist_ok=True)

# File names
main_log_file   = os.path.join(LOG_DIR, f"main_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
errors_log_file = os.path.join(LOG_DIR, f"ticket_creation_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# 1. Configure MAIN logger
main_logger = logging.getLogger("MainLogger")
main_logger.setLevel(logging.INFO)
if main_logger.hasHandlers():
    main_logger.handlers.clear()
main_handler = logging.FileHandler(main_log_file, encoding="utf-8")
main_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
main_logger.addHandler(main_handler)
main_logger.propagate = False

# 2. Configure ERROR logger (to separate file)
ticket_creation_error_logger = logging.getLogger("ticket_creation_errors")
ticket_creation_error_logger.setLevel(logging.INFO)
if ticket_creation_error_logger.hasHandlers():
    ticket_creation_error_logger.handlers.clear()
error_handler = logging.FileHandler(errors_log_file, encoding="utf-8")
error_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(message)s"))
ticket_creation_error_logger.addHandler(error_handler)
ticket_creation_error_logger.propagate = False

# Disable root logger console
logging.getLogger().handlers.clear()

