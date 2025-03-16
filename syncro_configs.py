import os
import logging
from datetime import datetime

# syncro_configs.py
SYNCRO_TIMEZONE = "America/New_York"
TICKETS_CSV_PATH = "tickets.csv"
COMMENTS_CSV_PATH = "ticket_comments.csv"
TEMP_FILE_PATH = "syncro_temp_data.json"
COMBINED_TICKETS_COMMENTS_CSV_PATH = "tickets_and_comments_combined.csv"

# Syncro API Configuration
SYNCRO_SUBDOMAIN = "hedgesmsp"
SYNCRO_API_KEY = "Te569accf4cb23c55f-285e3e6a36dfdaf1290951ede3262152"

SYNCRO_API_BASE_URL = f"https://{SYNCRO_SUBDOMAIN}.syncromsp.com/api/v1"

# Logging Configuration
LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "logs"))
os.makedirs(LOG_DIR, exist_ok=True)

# Define a fixed log file name instead of generating a new one each time
LOG_FILE_PATH = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d')}.log")


def setup_logging(log_level=logging.INFO):
    """Initialize logging with a specified log level."""
    logging.basicConfig(
        level=log_level,  # Set dynamically
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE_PATH, encoding="utf-8"),  # File logging
            logging.StreamHandler()  # Console logging
        ],
    )

def get_logger(name):
    return logging.getLogger(name)


