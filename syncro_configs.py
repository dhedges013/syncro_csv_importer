import os
import logging
from datetime import datetime

# syncro_configs.py
SYNCRO_TIMEZONE = "America/New_York"
LABOR_ENTRIES_CSV_PATH = "ticket_labor_entries.csv"
TEMP_FILE_PATH = "syncro_temp_data.json"
COMBINED_TICKETS_COMMENTS_CSV_PATH = "tickets_and_comments_combined.csv"

# Syncro API Configuration
SYNCRO_SUBDOMAIN = ""
SYNCRO_API_KEY = ""

SYNCRO_API_BASE_URL = f"https://{SYNCRO_SUBDOMAIN}.syncromsp.com/api/v1"

# Logging Configuration
LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "logs"))
os.makedirs(LOG_DIR, exist_ok=True)

# Define a fixed log file name instead of generating a new one each time
LOG_FILE_PATH = os.path.join(LOG_DIR, f"app_{datetime.now().strftime('%Y%m%d')}.log")

# Timestamp format configuration
# Options: "US" for MM/DD/YY or "INTL" for DD/MM/YY
TIMESTAMP_FORMAT = "US"

_TIMESTAMP_FORMATS = {
    "US": "%m/%d/%y %H:%M",
    "INTL": "%d/%m/%y %H:%M",
}


def is_day_first() -> bool:
    """Return True if configured to treat day as the first value in dates."""
    return TIMESTAMP_FORMAT.upper() == "INTL"


def get_timestamp_format() -> str:
    """Return the strftime/strptime format based on configuration."""
    return _TIMESTAMP_FORMATS["INTL"] if is_day_first() else _TIMESTAMP_FORMATS["US"]


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


