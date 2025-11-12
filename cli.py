# cli.py
import os
import glob
import json
from syncro_config_object import SyncroConfig
from syncro_configs import (
    setup_logging,
    get_logger,
    TEMP_FILE_PATH,
    TEMP_CREDENTIALS_FILE_PATH,
    SYNCRO_API_KEY,
    SYNCRO_SUBDOMAIN,
    LOG_DIR,
    LOG_FILE_PATH,
)
from main_tickets_comments_combined import run_tickets_comments_combined
from main_ticket_labor import run_ticket_labor
import logging

logger = get_logger(__name__)

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "default_config.json")
CLI_PREFERENCES_KEY = "cli_preferences"

LOG_LEVEL_CHOICES = {
    "1": ("DEBUG", logging.DEBUG),
    "2": ("INFO", logging.INFO),
    "3": ("WARNING", logging.WARNING),
    "4": ("ERROR", logging.ERROR),
    "5": ("CRITICAL", logging.CRITICAL),
}

LOG_LEVEL_NAME_TO_VALUE = {name: level for name, level in LOG_LEVEL_CHOICES.values()}


class DefaultConfigManager:
    """Lightweight helper to load and persist cli-specific answers."""

    def __init__(self, path: str = DEFAULT_CONFIG_PATH):
        self.path = path
        self.data = self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as file:
                return json.load(file)
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse %s: %s", self.path, exc)
        except OSError as exc:
            logger.error("Unable to read %s: %s", self.path, exc)
        return {}

    @property
    def preferences(self):
        return self.data.setdefault(CLI_PREFERENCES_KEY, {})

    def has_preferences(self) -> bool:
        return bool(self.preferences)

    def get_pref(self, key, default=None):
        return self.preferences.get(key, default)

    def set_pref(self, key, value):
        if value is None:
            self.preferences.pop(key, None)
        else:
            self.preferences[key] = value
        self._write()

    def _write(self):
        try:
            with open(self.path, "w", encoding="utf-8") as file:
                json.dump(self.data, file, indent=2)
        except OSError as exc:
            logger.error("Failed to update %s: %s", self.path, exc)


def prompt_yes_no(message: str, default_yes: bool = True) -> bool:
    """Generic yes/no prompt that honors a default selection."""
    hint = "Y/n" if default_yes else "y/N"
    while True:
        response = input(f"{message} ({hint}): ").strip().lower()
        if not response:
            return default_yes
        if response in ("y", "yes"):
            return True
        if response in ("n", "no"):
            return False
        print("Invalid selection. Please enter 'y' or 'n'.")


def resolve_boolean_choice(
    config_manager: DefaultConfigManager,
    pref_key: str,
    description: str,
    prompt_message: str,
    default_yes: bool,
    use_saved_answers: bool,
) -> bool:
    """
    Return a boolean answer, optionally reusing a stored preference and offering to persist new answers.
    """
    saved_pref = config_manager.get_pref(pref_key)

    if use_saved_answers and saved_pref is not None:
        print(f"Using saved choice for {description}: {'Yes' if saved_pref else 'No'}.")
        return saved_pref

    if saved_pref is not None:
        if prompt_yes_no(
            f"Use saved choice for {description}? (Current: {'Yes' if saved_pref else 'No'})",
            default_yes=True,
        ):
            return saved_pref

    decision = prompt_yes_no(prompt_message, default_yes=default_yes)
    should_persist = saved_pref is None or decision != saved_pref
    if should_persist and prompt_yes_no(f"Save this answer for {description} in future runs?", default_yes=True):
        config_manager.set_pref(pref_key, decision)
    return decision


# Function to get log level from user input

def get_log_level(config_manager: DefaultConfigManager, use_saved_answers: bool):
    saved_level = config_manager.get_pref("log_level")
    if saved_level and saved_level.upper() not in LOG_LEVEL_NAME_TO_VALUE:
        logger.warning("Saved log level '%s' is invalid. Falling back to prompt.", saved_level)
        saved_level = None

    if use_saved_answers and saved_level:
        print(f"Using saved logging level: {saved_level}")
        return LOG_LEVEL_NAME_TO_VALUE[saved_level.upper()]

    if saved_level:
        if prompt_yes_no(f"Use saved logging level '{saved_level}'?", default_yes=True):
            return LOG_LEVEL_NAME_TO_VALUE[saved_level.upper()]

    print("Select logging level:")
    for option, (name, _) in LOG_LEVEL_CHOICES.items():
        print(f"{option} - {name}")
    print("Press Enter for ALL (default: DEBUG)")

    while True:
        choice = input("Enter choice (1-5 or press Enter for DEBUG): ").strip()

        if choice == "":
            selected_name = "DEBUG"
        elif choice in LOG_LEVEL_CHOICES:
            selected_name = LOG_LEVEL_CHOICES[choice][0]
            print(f"Selected logging level: {selected_name}")
        else:
            print("Invalid choice. Please enter a number (1-5) or press Enter for default.")
            continue

        should_save = saved_level is None or selected_name != saved_level
        if should_save and prompt_yes_no("Save this logging level for future runs?", default_yes=True):
            config_manager.set_pref("log_level", selected_name)
        return LOG_LEVEL_NAME_TO_VALUE[selected_name]


def check_and_clear_temp_data(config_manager: DefaultConfigManager, use_saved_answers: bool):
    """Check if syncro_temp_data.json exists and prompt user to delete it."""
    if os.path.exists(TEMP_FILE_PATH):
        delete_file = resolve_boolean_choice(
            config_manager=config_manager,
            pref_key="delete_temp_data",
            description="temp data cleanup",
            prompt_message=f"File '{TEMP_FILE_PATH}' exists. Delete it now?",
            default_yes=False,
            use_saved_answers=use_saved_answers,
        )

        if delete_file:
            try:
                os.remove(TEMP_FILE_PATH)
                logger.info(f"Deleted {TEMP_FILE_PATH}")
                print(f"{TEMP_FILE_PATH} has been deleted.")
            except OSError as exc:
                logger.error("Failed to delete %s: %s", TEMP_FILE_PATH, exc)
                print(f"Unable to delete {TEMP_FILE_PATH}: {exc}")
        else:
            logger.info("User chose to keep the temp data file.")


def cleanup_old_logs(config_manager: DefaultConfigManager, use_saved_answers: bool):
    """Optionally delete prior app_*.log files to keep the logs directory tidy."""
    if not os.path.isdir(LOG_DIR):
        return

    log_pattern = os.path.join(LOG_DIR, "app_*.log")
    candidate_logs = [
        path
        for path in glob.glob(log_pattern)
        if os.path.abspath(path) != os.path.abspath(LOG_FILE_PATH)
    ]

    if not candidate_logs:
        return

    should_delete = resolve_boolean_choice(
        config_manager=config_manager,
        pref_key="cleanup_old_logs",
        description="old log cleanup",
        prompt_message="Delete existing log files from previous runs?",
        default_yes=False,
        use_saved_answers=use_saved_answers,
    )

    if not should_delete:
        logger.info("User chose not to delete old log files.")
        return

    deleted = 0
    for log_path in candidate_logs:
        try:
            os.remove(log_path)
            deleted += 1
        except OSError as exc:
            logger.error("Failed to delete log file %s: %s", log_path, exc)

    if deleted:
        logger.info("Deleted %d old log file(s).", deleted)
        print(f"Deleted {deleted} old log file(s).")
    else:
        logger.info("No old log files were deleted.")

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

def prompt_to_use_saved_credentials(config_manager: DefaultConfigManager, use_saved_answers: bool) -> bool:
    """Ask user whether to reuse cached credentials, honoring stored preferences."""
    return resolve_boolean_choice(
        config_manager=config_manager,
        pref_key="use_saved_credentials",
        description="using cached Syncro credentials",
        prompt_message="Saved Syncro credentials found. Use them?",
        default_yes=True,
        use_saved_answers=use_saved_answers,
    )

def prompt_to_save_credentials(
    config_manager: DefaultConfigManager,
    use_saved_answers: bool,
    subdomain: str,
    api_key: str,
):
    """Ask user if credentials should be cached for next run."""
    should_save = resolve_boolean_choice(
        config_manager=config_manager,
        pref_key="save_credentials",
        description="saving newly entered Syncro credentials",
        prompt_message="Do you want to save these credentials for the next run?",
        default_yes=True,
        use_saved_answers=use_saved_answers,
    )
    if should_save:
        save_credentials_for_next_run(subdomain, api_key)
    else:
        print("Credentials will not be saved.")

# If the API / Subdomain are not hard coded into the config file, prompt for them
def prompt_for_missing_credentials(config_manager: DefaultConfigManager, use_saved_answers: bool):
    if SYNCRO_API_KEY and SYNCRO_SUBDOMAIN:
        return SyncroConfig(SYNCRO_SUBDOMAIN, SYNCRO_API_KEY)

    saved_credentials = load_saved_credentials()
    if saved_credentials and prompt_to_use_saved_credentials(config_manager, use_saved_answers):
        logger.info("Using saved Syncro credentials from previous run.")
        subdomain, api_key = saved_credentials
        return SyncroConfig(subdomain, api_key)

    logger.info("Prompting user for API Key and Subdomain")
    subdomain = input("Enter your Syncro subdomain: ").strip()
    api_key = input("Enter your Syncro API Key: ").strip()
    prompt_to_save_credentials(config_manager, use_saved_answers, subdomain, api_key)
    return SyncroConfig(subdomain, api_key)

def main_menu():
    config_manager = DefaultConfigManager()
    use_saved_answers = False
    if config_manager.has_preferences():
        use_saved_answers = prompt_yes_no(
            "Saved CLI answers found in default_config.json. Use them for this run?",
            default_yes=True,
        )

    # Prompt user and set log level
    log_level = get_log_level(config_manager, use_saved_answers)
    setup_logging(log_level)
    logger.info("Logging to %s", LOG_FILE_PATH)
    cleanup_old_logs(config_manager, use_saved_answers)
    logger.critical("---------------------------------------------------")
    logger.critical("Starting Syncro Ticket Importer...")
    logger.critical("---------------------------------------------------")
    check_and_clear_temp_data(config_manager, use_saved_answers)
    config = prompt_for_missing_credentials(config_manager, use_saved_answers)
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

    
