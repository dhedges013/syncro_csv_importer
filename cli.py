# cli.py
import os
from syncro_config_object import SyncroConfig
from syncro_configs import setup_logging, get_logger, TEMP_FILE_PATH, SYNCRO_API_KEY, SYNCRO_SUBDOMAIN
from main_tickets import run_tickets
from main_comments import run_comments
from main_tickets_comments_combined import run_tickets_comments_combined
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

# If the API / Subdomain are not hard coded into the config file, prompt for them
def prompt_for_missing_credentials():
    if SYNCRO_API_KEY and SYNCRO_SUBDOMAIN:
        return SyncroConfig(SYNCRO_SUBDOMAIN, SYNCRO_API_KEY)
    else:
        logger.info("Prompting User for API Key and Subdomain")
        subdomain = input("Enter your Syncro subdomain: ")
        api_key = input("Enter your Syncro API Key: ")
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
    print("1. Tickets")
    print("2. Comments")
    print("3. Tickets and Comments Combined")
    choice = input("Enter 1, 2 or 3: ").strip()

    if choice == "1":
        run_tickets(config)
    elif choice == "2":
        run_comments(config)
    elif choice == "3":
        run_tickets_comments_combined(config)
    else:
        print("Invalid selection. Please enter 1, 2 or 3.")


if __name__ == "__main__":
    main_menu()

    