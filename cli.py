
# cli.py
from syncro_config_object import SyncroConfig
from syncro_configs import get_logger
from main_tickets import run

logger = get_logger(__name__)

def prompt_for_missing_credentials():
    logger.info("Prompting User for API Key and Subdomain")
    subdomain = input("Enter your Syncro subdomain: ")
    api_key = input("Enter your Syncro API Key: ")
    return SyncroConfig(subdomain, api_key)

def main_menu():
    config = prompt_for_missing_credentials()
    run(config)

if __name__ == "__main__":
    main_menu()

    