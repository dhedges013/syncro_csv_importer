# cli.py
import os
import json
from syncro_config_object import SyncroConfig
from syncro_configs import (
    setup_logging,
    get_logger,
    TEMP_FILE_PATH,
    TEMP_CREDENTIALS_FILE_PATH,
    SYNCRO_API_KEY,
    SYNCRO_SUBDOMAIN,
)
from main_tickets_comments_combined import run_tickets_comments_combined
from main_ticket_labor import run_ticket_labor
import logging

logger = get_logger(__name__)

# Function to get log level from user input

def get_log_level():
    log_levels = {
        "1": logging.DEBUG,
        "2": logging.INFO,
        "3": logging.WARNING,
        "4": logging.ERROR,
        "5": logging.CRITICAL
    }

    level_names = {
        "1": "DEBUG",
        "2": "INFO",
        "3": "WARNING",
        "4": "ERROR",
        "5": "CRITICAL"
    }

    print("Select logging level:")
    print("1 - DEBUG")
    print("2 - INFO")
    print("3 - WARNING")
    print("4 - ERROR")
    print("5 - CRITICAL")
    print("Press Enter for ALL (default: DEBUG)")

    while True:
        choice = input("Enter choice (1-5 or press Enter for DEBUG): ").strip()

        if choice == "":  # Default to DEBUG if Enter is pressed
            return logging.DEBUG
        
        if choice in log_levels:
            print(f"Selected logging level: {level_names[choice]}")
            return log_levels[choice]

        print("Invalid choice. Please enter a number (1-5) or press Enter for default.")

def check_and_clear_temp_data():
    """Check if syncro_temp_data.json exists and prompt user to delete it."""
    if os.path.exists(TEMP_FILE_PATH):
        while True:
            response = input(f"File '{TEMP_FILE_PATH}' exists. Do you want to delete it? (y/n): ").strip().lower()
            if response == "y":
                os.remove(TEMP_FILE_PATH)
                logger.info(f"Deleted {TEMP_FILE_PATH}")
                print(f"{TEMP_FILE_PATH} has been deleted.")
                break
            elif response == "n":
                logger.info("User chose to keep the temp data file.")
                break
            else:
                print("Invalid selection. Please enter 'y' for Yes or 'n' for No.")

def load_saved_credentials():
    """Load cached credentials if available."""
    if not os.path.exists(TEMP_CREDENTIALS_FILE_PATH):
        return None
    try:
        with open(TEMP_CREDENTIALS_FILE_PATH, "r", encoding="utf-8") as file:
            data = json.load(file)
        subdomain = data.get("subdomain")
        api_key = data.get("api_key")
        if subdomain and api_key:
            return subdomain, api_key
        logger.warning("Saved credentials file is missing required values. Re-entering credentials.")
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Unable to read saved credentials: %s", exc)
    return None

def save_credentials_for_next_run(subdomain: str, api_key: str):
    """Persist credentials so the next run can reuse them."""
    try:
        with open(TEMP_CREDENTIALS_FILE_PATH, "w", encoding="utf-8") as file:
            json.dump({"subdomain": subdomain, "api_key": api_key}, file)
        logger.info("Saved Syncro credentials for next run.")
    except OSError as exc:
        logger.error("Failed to save credentials for next run: %s", exc)

def prompt_to_use_saved_credentials() -> bool:
    """Ask user whether to reuse cached credentials."""
    while True:
        response = input("Saved Syncro credentials found. Use them? (Y/n): ").strip().lower()
        if response in ("", "y", "yes"):
            return True
        if response in ("n", "no"):
            return False
        print("Invalid selection. Please enter 'y' for Yes or 'n' for No.")

def prompt_to_save_credentials(subdomain: str, api_key: str):
    """Ask user if credentials should be cached for next run."""
    while True:
        response = input("Do you want to save these credentials for the next run? (y/n): ").strip().lower()
        if response in ("y", "yes"):
            save_credentials_for_next_run(subdomain, api_key)
            break
        if response in ("n", "no", ""):
            print("Credentials will not be saved.")
            break
        print("Invalid selection. Please enter 'y' for Yes or 'n' for No.")

# If the API / Subdomain are not hard coded into the config file, prompt for them
def prompt_for_missing_credentials():
    if SYNCRO_API_KEY and SYNCRO_SUBDOMAIN:
        return SyncroConfig(SYNCRO_SUBDOMAIN, SYNCRO_API_KEY)

    saved_credentials = load_saved_credentials()
    if saved_credentials and prompt_to_use_saved_credentials():
        logger.info("Using saved Syncro credentials from previous run.")
        subdomain, api_key = saved_credentials
        return SyncroConfig(subdomain, api_key)

    logger.info("Prompting user for API Key and Subdomain")
    subdomain = input("Enter your Syncro subdomain: ").strip()
    api_key = input("Enter your Syncro API Key: ").strip()
    prompt_to_save_credentials(subdomain, api_key)
    return SyncroConfig(subdomain, api_key)

def main_menu():
    # Prompt user and set log level
    log_level = get_log_level()
    setup_logging(log_level)
    logger.critical("---------------------------------------------------")
    logger.critical("Starting Syncro Ticket Importer...")
    logger.critical("---------------------------------------------------")
    check_and_clear_temp_data()
    config = prompt_for_missing_credentials()
    print("Choose your importer:")
    print("1. Tickets and Comments Combined")
    print("2. Ticket Labor Entries")
    choice = input("Enter 1 or 2: ").strip()

    if choice == "1":
        run_tickets_comments_combined(config)
    elif choice == "2":
        run_ticket_labor(config)
    else:
        print("Invalid selection. Please enter 1 or 2.")


if __name__ == "__main__":
    main_menu()

    
