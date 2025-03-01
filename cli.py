# cli.py
import os
from syncro_config_object import SyncroConfig
from syncro_configs import get_logger, TEMP_FILE_PATH, SYNCRO_API_KEY, SYNCRO_SUBDOMAIN
from main_tickets import run

logger = get_logger(__name__)


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

def prompt_for_missing_credentials():
    if SYNCRO_API_KEY and SYNCRO_SUBDOMAIN:
        return SyncroConfig(SYNCRO_SUBDOMAIN, SYNCRO_API_KEY)
    else:
        logger.info("Prompting User for API Key and Subdomain")
        subdomain = input("Enter your Syncro subdomain: ")
        api_key = input("Enter your Syncro API Key: ")
        return SyncroConfig(subdomain, api_key)

def main_menu():
    check_and_clear_temp_data()
    config = prompt_for_missing_credentials()
    run(config)

if __name__ == "__main__":
    main_menu()

    