import os
from datetime import datetime, timedelta
from dateutil import parser
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import csv
import pytz
from collections import defaultdict

from syncro_configs import (
    get_logger,
    TEMP_FILE_PATH,
    SYNCRO_TIMEZONE,
    COMBINED_TICKETS_COMMENTS_CSV_PATH,
    LABOR_ENTRIES_CSV_PATH,
    is_day_first,
    get_timestamp_format,
)

from syncro_read import (
    syncro_get_all_techs,
    syncro_get_issue_types,
    syncro_get_all_customers,
    syncro_get_all_contacts,
    syncro_get_ticket_statuses,
    syncro_get_all_products
)

_temp_data_cache = None  # Global cache for temp data

# Get a logger for this module
logger = get_logger(__name__)


DEFAULT_CONFIG_PATH = "default_config.json"


def load_default_config(path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load default value mappings from a JSON configuration file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Default config file not found: {path}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse default config: {e}")
    return {}


DEFAULTS = load_default_config()


def validate_customers(customers: Optional[List[Dict[str, Any]]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Drop customers with blank business names and log summary statistics.

    Returns a sanitized customer list plus a dict containing counts for
    processed, dropped, auto-filled, and whitespace-trimmed records.
    """
    if not customers:
        logger.info("Customer validation complete: processed 0 records; dropped 0; auto-filled 0; trimmed 0.")
        return [], {"processed": 0, "dropped": 0, "auto_filled": 0, "trimmed": 0}

    sanitized_customers: List[Dict[str, Any]] = []
    dropped = 0
    auto_filled = 0  # Placeholder for future use if we decide to backfill blank names.
    trimmed = 0

    for customer in customers:
        business_name = customer.get("business_name")
        normalized_name = (business_name or "").strip()

        if not normalized_name:
            dropped += 1
            logger.debug(
                "Dropping customer with blank business_name: id=%s, raw_payload=%s",
                customer.get("id"),
                customer,
            )
            continue

        sanitized_customer = dict(customer)
        if sanitized_customer.get("business_name") != normalized_name:
            sanitized_customer["business_name"] = normalized_name
            trimmed += 1

        sanitized_customers.append(sanitized_customer)

    processed = len(customers)
    logger.info(
        "Customer validation complete: processed %s records; dropped %s blank names; auto-filled %s; trimmed %s.",
        processed,
        dropped,
        auto_filled,
        trimmed,
    )

    stats = {
        "processed": processed,
        "dropped": dropped,
        "auto_filled": auto_filled,
        "trimmed": trimmed,
    }
    return sanitized_customers, stats

def load_or_fetch_temp_data(config=None) -> dict:
    """
    Load temp data from a file or fetch from Syncro API if file doesn't exist

    Args:
        logger (logging.Logger): Logger instance for logging.       

    Returns:
        dict: Dictionary containing techs, issue types, customers, and contacts.
    """
    global _temp_data_cache  # Use a global variable to cache temp data

    # Check if data is already cached in memory
    if _temp_data_cache:
        logger.debug("Using cached temp data.")
        return _temp_data_cache

    # Check if temp file exists
    if os.path.exists(TEMP_FILE_PATH):
        try:
            logger.info(f"Loading temp data from {TEMP_FILE_PATH}")
            with open(TEMP_FILE_PATH, "r") as file:
                cached_data = json.load(file)

            validated_customers, stats = validate_customers(cached_data.get("customers", []))
            cached_data["customers"] = validated_customers
            _temp_data_cache = cached_data

            if stats["dropped"] or stats["auto_filled"] or stats["trimmed"]:
                try:
                    with open(TEMP_FILE_PATH, "w") as file:
                        json.dump(_temp_data_cache, file)
                    logger.info(
                        "Persisted sanitized customer data back to %s after removing invalid entries.",
                        TEMP_FILE_PATH,
                    )
                except OSError as write_error:
                    logger.error(f"Failed to persist sanitized temp data: {write_error}")

            return _temp_data_cache
        except Exception as e:
            logger.error(f"Failed to load temp data from file: {e}")

    # Fetch data from Syncro API 
    logger.info("Fetching data from Syncro API...")
    try:
        techs = syncro_get_all_techs(config)
        issue_types = syncro_get_issue_types(config)
        customers, _ = validate_customers(syncro_get_all_customers(config))
        contacts = syncro_get_all_contacts(config)
        statuses = syncro_get_ticket_statuses(config)
        products = syncro_get_all_products(config)

        _temp_data_cache = {
            "techs": techs,
            "issue_types": issue_types,
            "customers": customers,
            "contacts": contacts,
            "statuses": statuses,
            "products": products,
        }

        # Save to temp file
        logger.info(f"Saving temp data to {TEMP_FILE_PATH}")
        with open(TEMP_FILE_PATH, "w") as file:
            json.dump(_temp_data_cache, file)

    except Exception as e:
        logger.error(f"Failed to fetch data from Syncro API or save temp data: {e}")
        raise

    return _temp_data_cache


def get_customer_id_by_name(customer_name: str, config: Dict[str, Any]):#, logger: logging.Logger) -> int:
    """
    Retrieve customer ID from temp data based on matching customer name.

    Args:
        customer_name (str): Customer name to search for.
        logger (logging.Logger): Logger instance for logging.

    Returns:
        int: Customer ID if found, otherwise None.

    Logs:
        - Info for successful matches.
        - Warning if no match is found.
        - Error if an issue occurs during execution.
    """
    try:
        # Load temp data
        temp_data = load_or_fetch_temp_data(config=config)
        customers = temp_data.get("customers", [])

        if not customers:
            logger.warning("No customer data available. Ensure temp data is correctly loaded.")
            return None

        # Normalize input for case-insensitive comparison
        normalized_customer_name = customer_name.strip().lower()
        logger.debug(f"Normalized customer name: passed in as {customer_name} but is now {normalized_customer_name}")

        # Search for the customer by name
        for customer in customers:
            customer_name_in_list = customer.get("business_name", "").strip().lower()
            if customer_name_in_list == normalized_customer_name:
                customer_id = customer.get("id")
                logger.debug(f"Match found: Customer '{customer_name}' matches '{customer['business_name']}' with ID {customer_id}")
                return customer_id

        logger.warning(f"Customer not found: {customer_name}")
        return None

    except KeyError as e:
        logger.error(f"Key error while accessing customer data: {e}")
        return None

    except Exception as e:
        logger.error(f"An unexpected error occurred in get_customer_id_by_name: {e}")
        return None
 
def check_duplicate_customer(config,customer_name: str) -> bool:
    """
    Check if a customer with the given name already exists using temp data.

    Args:
        customer_name (str): Name of the customer to check.
        logger (logging.Logger): Logger instance for logging.

    Returns:
        bool: True if the customer exists, False otherwise.

    Logs:
        - Info for successful checks.
        - Warning if a duplicate customer is found.
        - Error if any issue occurs during execution.
    """
    try:
        # Load temp data
        temp_data = load_or_fetch_temp_data(config)
        customers = temp_data.get("customers", [])

        if not customers:
            logger.warning("No customer data available. Ensure temp data is correctly loaded.")
            return False

        # Normalize input for case-insensitive comparison
        normalized_customer_name = customer_name.strip().lower()

        # Extract and normalize business names from customers
        business_names = [customer.get("business_name", "").strip().lower() for customer in customers]

        logger.debug(f"Retrieved normalized business names: {business_names}")
        logger.debug(f"Checking for duplicate customer: {customer_name}")

        # Check for duplicate
        if normalized_customer_name in business_names:
            logger.warning(f"Duplicate customer found: {customer_name}")
            return True

        logger.debug(f"No duplicate found for customer: {customer_name}")
        return False

    except KeyError as e:
        logger.error(f"Key error while accessing customer data: {e}")
        return False

    except Exception as e:
        logger.error(f"An unexpected error occurred in check_duplicate_customer: {e}")
        return False
def check_duplicate_contact(contact_name: str, logger: logging.Logger) -> bool:
    """
    Check if a contact with the given name already exists using temp data.

    Args:
        contact_name (str): Name of the contact to check.
        logger (logging.Logger): Logger instance for logging.

    Returns:
        bool: True if the contact exists, False otherwise.

    Logs:
        - Info for successful checks.
        - Warning if a duplicate contact is found.
        - Error if any issue occurs during execution.
    """
    try:
        # Load temp data
        temp_data = load_or_fetch_temp_data()
        contacts = temp_data.get("contacts", [])

        if not contacts:
            logger.warning("No contact data available. Ensure temp data is correctly loaded.")
            return False

        # Normalize input for case-insensitive comparison
        normalized_contact_name = contact_name.strip().lower()

        # Extract and normalize contact names
        contact_names = [contact.get("name", "").strip().lower() for contact in contacts]

        logger.debug(f"Retrieved normalized contact names: {contact_names}")
        logger.debug(f"Checking for duplicate contact: {contact_name}")

        # Check for duplicate
        if normalized_contact_name in contact_names:
            logger.warning(f"Duplicate contact found: {contact_name}")
            return True

        logger.debug(f"No duplicate found for contact: {contact_name}")
        return False

    except KeyError as e:
        logger.error(f"Key error while accessing contact data: {e}")
        return False

    except Exception as e:
        logger.error(f"An unexpected error occurred in check_duplicate_contact: {e}")
        return False

def extract_nested_key(data: dict, key_path: str):
    """
    Extract a nested key from a dictionary using dot notation.

    Args:
        data (dict): Dictionary to search.
        key_path (str): Dot-separated path to the key.

    Returns:
        Any: Value of the nested key if it exists, otherwise None.
    """
    keys = key_path.split('.')
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return None
    return data

def load_csv(filepath: str, required_fields: List[str] = None, logger: logging.Logger = None) -> List[Dict[str, Any]]:
    """
    Load data from a CSV file with validation for required fields.
    Blank values for keys present in ``DEFAULTS`` are filled with their
    configured defaults instead of raising a validation error.

    Args:
        filepath (str): The path to the CSV file.
        required_fields (List[str]): List of required field names to validate.
        logger (logging.Logger, optional): Logger instance for logging.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents a row in the CSV file.

    Raises:
        FileNotFoundError: If the file is not found.
        ValueError: If required fields are missing or if any row data is blank.
    """
    if logger is None:
        logger = logging.getLogger("syncro")

    try:
        logger.debug(f"Loading data from CSV file: {filepath}")
        with open(filepath, mode="r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            headers = reader.fieldnames or []
            headers_lower = [h.lower() for h in headers]

            if required_fields:
                required_map = {field.lower(): field for field in required_fields}
                required_lower = list(required_map.keys())
                missing_fields = [required_map[field_lower] for field_lower in required_lower if field_lower not in headers_lower]
                if missing_fields:
                    raise ValueError(f"Missing required fields in CSV file: {missing_fields}")

            data = []
            for row_number, row in enumerate(reader, start=1):
                cleaned_row = {}
                for key, value in row.items():
                    if key is None:
                        message = (
                            "Row {row_number}: Encountered a column without a header while "
                            "processing '{filepath}'. Value: '{value}'. Ensure the CSV matches the "
                            "expected template."
                        )
                        logger.error(message.format(row_number=row_number, filepath=filepath, value=value))
                        raise ValueError(
                            f"Row {row_number}: Found column without header while reading {filepath}."
                        )

                    key_lower = key.lower()
                    if value is None or value.strip() == "":
                        default_value = DEFAULTS.get(key_lower)
                        if default_value is not None:
                            logger.info(
                                f"Row {row_number}: Field '{key}' is blank, applying default '{default_value}'."
                            )
                            value = default_value
                        else:
                            raise ValueError(f"Row {row_number}: Empty value found in field '{key}'.")
                    cleaned_row[key_lower] = value

                if required_fields:
                    for field_lower in required_lower:
                        if field_lower not in cleaned_row or cleaned_row[field_lower].strip() == "":
                            raise ValueError(
                                f"Row {row_number}: Missing or blank required field '{required_map[field_lower]}'."
                            )

                data.append(cleaned_row)

            logger.debug(f"Successfully loaded {len(data)} rows from {filepath}.")
            return data

    except FileNotFoundError:
        logger.error(f"CSV file not found: {filepath}")
        raise
    except ValueError as e:
        logger.error(f"Validation error in CSV file: {e}")
        raise
    except Exception as e:
        logger.exception(f"Error reading CSV file {filepath}: {e}")
        raise
def validate_ticket_data(tickets: List[Dict[str, Any]], temp_data: Dict[str, Any], logger: logging.Logger) -> None:

    logger.debug("Validating ticket data...")

    # Extract needed lists from temp_data
    techs = temp_data.get("techs", [])
    customers = temp_data.get("customers", [])
    issue_types = temp_data.get("issue_types", [])
    statuses = temp_data.get("statuses", [])
    contacts = temp_data.get("contacts", [])
    logger.debug(f"Retrieved techs: {techs}, issue_types: {issue_types}, "
                f"statuses: {statuses}, Ignoring Customers and contacts due to long load")
    
    # Build sets of names/values to compare against
    try:
        tech_names = {t[1].lower() for t in techs}
    except Exception as e:
        logger.error(f"Error extracting tech names: {e}")
        raise
    try:                           
        customer_names = {c["business_name"].lower() for c in customers}
    except Exception as e:
        logger.error(f"Error extracting customer names: {e}")
        raise
    try:
        issue_type_names = {i.lower() for i in issue_types}
    except Exception as e:
        logger.error(f"Error extracting issue type names: {e}")
        raise
    try:
        status_names = {s for s in statuses} #you can not normalize status names
    except Exception as e:
        logger.error(f"Error extracting status names: {e}")
        raise    
    try:
        contact_names = {c["name"].lower() for c in contacts if c["name"]}
    except Exception as e:
        logger.error(f"Error extracting contact names: {e}")
        raise

    for row_num, ticket in enumerate(tickets, start=1):
        logger.debug(f"Validation for Row {row_num} - Raw ticket data: {ticket}")

        # Retrieve each field from the ticket
        tech_val = ticket["tech"].strip().lower()
        customer_val = ticket["ticket customer"].strip().lower()
        issue_type_val = ticket["ticket issue type"].strip().lower()
        status_val = ticket["ticket status"] #you can not normalize status names. Must be perfect match
        contact_val = ticket.get("ticket contact").strip().lower()  # or "contact", if that's the CSV header

        logger.debug(f"Validation Row {row_num} - Checking tech='{tech_val}', customer='{customer_val}', "
                     f"issue_type='{issue_type_val}', status='{status_val}', contact='{contact_val}'")

        # Check Tech
        logger.debug(f"Validation Row {row_num}: Checking tech '{tech_val}' against {tech_names}")

        if tech_val not in tech_names:
            logger.error(f"Row {row_num}: Tech '{tech_val}' not found in API cache.")
            raise ValueError(f"Row {row_num}: Tech '{tech_val}' not found in API cache.")

        # Check Customer
        logger.debug(f"Validation Row {row_num}: Checking customer val '{customer_val}' against {customer_names}")
        if customer_val not in customer_names:
            logger.error(f"Row {row_num}: Customer '{customer_val}' not found in API cache.")
            raise ValueError(f"Row {row_num}: Customer '{customer_val}' not found in API cache.")

        # Check Issue Type
        logger.debug(f"Validation Row {row_num}: Checking issue type val '{issue_type_val}' against {issue_type_names}")
        if issue_type_val not in issue_type_names:
            logger.error(f"Row {row_num}: Issue type '{issue_type_val}' not found in API cache.")
            raise ValueError(f"Row {row_num}: Issue type '{issue_type_val}' not found in API cache.")

        # Check Status
        logger.debug(f"Validation Row {row_num}: Checking status  val '{status_val}' against {status_names}")
        if status_val not in status_names:
            logger.warning("Status names cannot be normalized. Must be perfect match")
            logger.error(f"Row {row_num}: Status '{status_val}' not found in API cache.")
            raise ValueError(f"Row {row_num}: Status '{status_val}' not found in API cache.")

        # Check Contact (warn if missing, but don’t error out)
        if contact_val not in contact_names:
            logger.warning(f"Row {row_num}: Contact '{contact_val}' not found in API cache.")

        logger.debug(f"Validation Row {row_num} - Validation passed for this ticket.")

    logger.info("All tickets validated successfully.")

def clean_syncro_ticket_number(ticketNumber: str) -> str:
    """
    Cleans the ticket number to ensure it contains only numeric characters.

    Args:
        ticketNumber (str): The input ticket number.

    Returns:
        str: A string with only numeric characters from the input.

    Logs:
        - Info if ticket number is cleaned successfully.
        - Error if an unexpected issue occurs.
    """
    try:
        # Remove any non-numeric characters
        cleaned_ticket_number = ''.join(filter(str.isdigit, ticketNumber))

        # Log the original and cleaned ticket number
        logger.debug(f"Original ticket number: {ticketNumber}")
        logger.debug(f"Cleaned ticket number: {cleaned_ticket_number}")

        return cleaned_ticket_number

    except Exception as e:
        # Log the error and raise it for further handling
        logger.error(f"Error processing ticket number '{ticketNumber}': {e}")
        return None

def get_syncro_tech(tech_name: str):
    """
    Get the ID of a technician by name (case-insensitive).

    Args:
        tech_name (str): Name of the technician to search for.
        logger (logging.Logger): Logger instance for logging.

    Returns:
        str: Technician ID, or None if not found.
    """
    try:
        # Load temp data
        temp_data = load_or_fetch_temp_data()
        techs = temp_data.get("techs", [])

        # Check if tech data exists
        if not techs:
            logger.error("No technician data available. Ensure temp data is correctly loaded.")
            return None

        # Normalize input to lowercase for case-insensitive comparison
        normalized_tech_name = tech_name.strip().lower()

        # Search for the technician by name (case-insensitive)
        for tech in techs:
            tech_id = None
            tech_name_in_list = None

            if isinstance(tech, dict):
                # If entry is a dictionary, extract fields using keys
                tech_id = tech.get("id")
                tech_name_in_list = tech.get("name", "").strip().lower()
            elif isinstance(tech, list) and len(tech) >= 2:
                # If entry is a list, assume [id, name] structure
                tech_id, tech_name_in_list_raw = tech[0], tech[1]
                tech_name_in_list = str(tech_name_in_list_raw).strip().lower()
            else:
                # Log and skip unexpected entry formats
                logger.warning(f"Unexpected tech entry format: {tech}. Skipping entry.")
                continue

            if tech_name_in_list == normalized_tech_name:
                logger.debug(f"Match found: Tech '{tech_name}' matches '{tech_name_in_list}' with ID {tech_id}")
                return str(tech_id)

        # Log a warning if the technician is not found
        logger.warning(f"Technician not found: {tech_name}")
        return None

    except KeyError as e:
        logger.error(f"Key error while accessing tech data: {e}")
        return None

    except Exception as e:
        logger.error(f"An unexpected error occurred in get_syncro_tech: {e}")
        return None

def build_syncro_initial_issue(initial_issue: str, syncroContact: str, created_at: Optional[str] = None) -> list:
    """
    Build the JSON object for the initial issue in Syncro.

    Args:
        initial_issue (str): The issue description.
        syncroContact (str): The Syncro contact associated with the issue.

    Returns:
        list: A list representing the comments for the initial issue.

    Logs:
        - Info for successfully built JSON object.
        - Error if inputs are invalid or unexpected issues occur.
    """
    import json
    try:
        # Validate inputs
        if not initial_issue:
            raise ValueError("Error: 'initial_issue' must be provided.")      
        if not syncroContact:
            syncroContact = "None"
            logger.warning(f"No Contract was provided setting Ticket Contact to None")        

        # Build the JSON structure as a list of comments
        initial_issue_comments = []
        comment = {
                "subject": "Initial Issue",
                "body": initial_issue,
                "hidden": True,
                "do_not_email": True,
                "tech": syncroContact  }

        if created_at:
            comment["created_at"] = created_at

        initial_issue_comments.append(comment)
        # Log the built JSON
        logger.info(f"Successfully built initial issue comments: {initial_issue_comments}")
        return initial_issue_comments

    except ValueError as ve:
        # Log value errors
        logger.error(f"Input validation error: {ve}")
        raise
    except Exception as e:
        # Log unexpected errors
        logger.error(f"Unexpected error occurred while building initial issue comments: {e}")
        raise

def get_syncro_created_date(created: str) -> str:
    """
    Process a date or string that looks like a date and reformat it to ISO 8601 format with the local timezone.

    Args:
        created (str): Input date string.

    Returns:
        str: Reformatted date string in ISO 8601 format with timezone offset (e.g., 2024-12-15T00:00:00-05:00).

    Logs:
        - Info for successfully parsed and formatted dates.
        - Error if the input cannot be processed.
    """
    try:
        logger.debug(f"Attempting to parse and format date: {created}")

        parsed_date = None

        if isinstance(created, datetime):
            parsed_date = created
        else:
            # Try using dateutil's parser first for flexibility
            try:
                parsed_date = parser.parse(
                    created,
                    dayfirst=is_day_first(),
                    fuzzy=True,
                )
                logger.debug(
                    "Parsed datetime using dateutil with dayfirst=%s: %s",
                    is_day_first(),
                    parsed_date,
                )
            except (ValueError, TypeError) as e:
                logger.warning(f"dateutil parser failed: {e}")

        if parsed_date is None:
            separators = ["/", "-", "."]
            formats = [
                "%Y-%m-%d",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y/%m/%d",
                "%Y/%m/%d %H:%M",
                "%Y-%m-%dT%H:%M:%S",
            ]

            if is_day_first():
                base_patterns = [
                    "%d{sep}%m{sep}%Y %H:%M:%S",
                    "%d{sep}%m{sep}%Y %H:%M",
                    "%d{sep}%m{sep}%Y %I:%M %p",
                    "%d{sep}%m{sep}%y %H:%M:%S",
                    "%d{sep}%m{sep}%y %H:%M",
                    "%d{sep}%m{sep}%y %I:%M %p",
                    "%d{sep}%m{sep}%Y",
                    "%d{sep}%m{sep}%y",
                ]
            else:
                base_patterns = [
                    "%m{sep}%d{sep}%Y %H:%M:%S",
                    "%m{sep}%d{sep}%Y %H:%M",
                    "%m{sep}%d{sep}%Y %I:%M %p",
                    "%m{sep}%d{sep}%y %H:%M:%S",
                    "%m{sep}%d{sep}%y %H:%M",
                    "%m{sep}%d{sep}%y %I:%M %p",
                    "%m{sep}%d{sep}%Y",
                    "%m{sep}%d{sep}%y",
                ]

            for sep in separators:
                for pattern in base_patterns:
                    formats.append(pattern.format(sep=sep))

            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(created, fmt)
                    logger.debug("Parsed datetime using format '%s': %s", fmt, parsed_date)
                    break
                except ValueError:
                    continue

        if parsed_date is None:
            raise ValueError(f"Unrecognized date format: {created}")

        # Set time to midnight if not specified
        if parsed_date.hour == 0 and parsed_date.minute == 0 and parsed_date.second == 0:
            logger.warning(f"Time missing, setting to midnight: {parsed_date}")

        # Localize the date to SYNCRO_TIMEZONE
        local_timezone = pytz.timezone(SYNCRO_TIMEZONE)
        localized_date = local_timezone.localize(parsed_date)

        # Format the date with timezone offset
        formatted_date = localized_date.strftime("%Y-%m-%dT%H:%M:%S%z")

        logger.debug(f"Formatted date with timezone offset: {formatted_date}")
        return formatted_date

    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        raise
    except Exception as e:
        logger.error(f"Error processing date '{created}': {e}")
        raise

def get_syncro_customer_contact(customerid: Optional[str], contact: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Resolve a contact record for a given customer and contact name.

    Returns a dict containing at least ``id`` and ``name`` when a match is
    found, otherwise ``None``.
    """
    try:
        if not contact or not str(contact).strip():
            logger.warning("Contact lookup skipped: blank contact value for customer ID %s.", customerid)
            return None

        if not customerid:
            logger.error("Contact lookup requires a customer ID; received %s.", customerid)
            return None

        temp_data = load_or_fetch_temp_data()
        contacts_data = temp_data.get("contacts", [])

        normalized_customer_id = str(customerid)
        customer_contacts = [
            c for c in contacts_data if str(c.get("customer_id")) == normalized_customer_id
        ]

        if not customer_contacts:
            logger.warning("No contacts found for customer ID %s.", normalized_customer_id)
            return None

        normalized_input_contact = str(contact).strip().lower()
        normalized_filtered_contacts = {}

        for record in customer_contacts:
            raw_name = record.get("name")
            contact_id = record.get("id")

            if contact_id is None or not raw_name:
                logger.debug(
                    "Skipping contact record missing id/name for customer %s: %s",
                    normalized_customer_id,
                    record,
                )
                continue

            normalized_name = str(raw_name).strip().lower()
            normalized_filtered_contacts[normalized_name] = {
                "id": contact_id,
                "name": str(raw_name).strip(),
            }

        logger.debug(
            "Prepared %s contacts for lookup against normalized name '%s'.",
            len(normalized_filtered_contacts),
            normalized_input_contact,
        )

        match = normalized_filtered_contacts.get(normalized_input_contact)
        if match:
            logger.debug(
                "Matched contact '%s' to ID %s for customer %s.",
                contact,
                match["id"],
                normalized_customer_id,
            )
            return match

        logger.warning(
            "No exact match for contact '%s' (customer %s).",
            contact,
            normalized_customer_id,
        )
        return None

    except Exception as e:
        logger.error(
            "Error occurred while finding contact '%s' for customer ID %s: %s",
            contact,
            customerid,
            e,
        )
        raise





def get_syncro_priority(priority: str) -> str:
    """
    Match a given priority string with the corresponding Syncro priority.

    Args:
        priority (str): The priority level (e.g., 'Urgent', 'High', 'Normal', 'Low').

    Returns:
        str: The matched priority string (e.g., '1 High').

    Logs:
        - Info for successful matching.
        - Warning if no match is found.
    """
    if not priority:
            logger.warning(f"Priority is missing or None, Setting priority to 'Normal' by default.")
            priority = "normal"
    try:
        # Define priority mappings
        priority_map = {
            "urgent": "0 Urgent",
            "high": "1 High",
            "normal": "2 Normal",
            "low": "3 Low"
        }

        # Normalize the input to lowercase for case-insensitive matching
        normalized_priority = priority.strip().lower()
        # Attempt to find the match
        matched_priority = priority_map.get(normalized_priority)

        if matched_priority:
            logger.debug(f"Priority '{priority}' matched to '{matched_priority}'")
            return matched_priority
        else:
            logger.warning(f"NON Standard Priority was passed in: {priority}, defaulting to '2 Normal'.")
            return "2 Normal"

    except Exception as e:
        # Log any unexpected errors
        logger.error(f"Error occurred while matching priority '{priority}': {e}")
        raise

def get_syncro_issue_type(issue_type: str):
    """
    Match the given issue type with Syncro issue types and return the matched type.

    Args:
        issue_type (str): The issue type to find.
        logger (logging.Logger): Logger instance for logging.

    Returns:
        str: The matched Syncro issue type if found, otherwise None.

    Logs:
        - Info for successful matching.
        - Warning if no match is found.
        - Error if any issue occurs during execution.
    """
    try:
        # Load temp data
        temp_data = load_or_fetch_temp_data()
        issue_types = temp_data.get("issue_types", [])

        if not issue_types:
            logger.warning("No issue types found in Syncro settings. Returning default")
            return DEFAULTS.get("ticket issue type", "Other")

        # Normalize the input for case-insensitive comparison
        if not issue_type:
            issue_type = DEFAULTS.get("ticket issue type", "Other")
        normalized_issue_type = issue_type.strip().lower()

        # Search for a match in the retrieved issue types
        for syncro_issue_type in issue_types:
            if syncro_issue_type.strip().lower() == normalized_issue_type:
                logger.debug(
                    f"Match found: Input '{issue_type}' matches Syncro issue type '{syncro_issue_type}'."
                )
                return syncro_issue_type

        # Log a warning if no match is found and use default
        logger.warning(f"No match found for issue type: {issue_type}. Using default")
        return DEFAULTS.get("ticket issue type", "Other")

    except KeyError as e:
        logger.error(f"Key error while accessing issue types: {e}")
        return None

    except Exception as e:
        logger.error(f"Error occurred while matching issue type '{issue_type}': {e}")
        return None

def get_syncro_product_id_by_name(product_name: str, config=None) -> Optional[int]:
    """Return the product ID that matches ``product_name`` (case-insensitive)."""

    if not product_name:
        logger.debug("No product name provided for lookup.")
        return None

    try:
        temp_data = load_or_fetch_temp_data(config=config)
        products = temp_data.get("products", [])

        if not products:
            logger.warning("Product list is empty; unable to match labor type to a product.")
            return None

        normalized_name = product_name.strip().lower()

        for product in products:
            if isinstance(product, dict):
                name = str(product.get("name", "")).strip().lower()
                if name == normalized_name:
                    product_id = product.get("id")
                    logger.debug(f"Matched product '{product_name}' to ID '{product_id}'.")
                    return product_id

        logger.warning(f"Unable to match labor type '{product_name}' to a Syncro product.")
        return None

    except Exception as e:
        logger.error(f"Unexpected error while looking up product '{product_name}': {e}")
        return None

def parse_visibility_value(visibility: Optional[str]) -> Optional[bool]:
    """Convert human-friendly visibility strings into Syncro's hidden flag."""

    if visibility is None:
        return None

    normalized = visibility.strip().lower()
    if normalized in {"private", "internal", "hidden"}:
        return True
    if normalized in {"public", "customer", "external"}:
        return False

    logger.warning(f"Unrecognized visibility value '{visibility}'.")
    return None

def parse_billable_status(status: Optional[str]) -> Optional[bool]:
    """Convert billable status text into a boolean override flag."""

    if status is None:
        return None

    normalized = status.strip().lower()
    if normalized in {"billable", "billed"}:
        return True
    if normalized in {"non-billable", "non billable", "not billable", "unbillable"}:
        return False

    logger.warning(f"Unrecognized billable status '{status}'.")
    return None

def syncro_get_all_ticket_labor_entries_from_csv() -> List[Dict[str, Any]]:
    """Load ticket labor entries from CSV with validation."""

    required_fields = [
        "customer",
        "ticket number",
        "entry sequence",
        "tech",
        "duration minutes",
        "visibility",
        "billable status",
        "labor type",
        "created at",
        "notes",
    ]

    try:
        logger.info("Attempting to load ticket labor entries from CSV...")
        entries = load_csv(LABOR_ENTRIES_CSV_PATH, required_fields=required_fields, logger=logger)
        logger.info(
            f"Successfully loaded {len(entries)} labor entries from {LABOR_ENTRIES_CSV_PATH}."
        )
        return entries

    except FileNotFoundError:
        logger.error(f"CSV file not found: {LABOR_ENTRIES_CSV_PATH}")
        raise

    except ValueError as e:
        logger.error(f"Validation error in CSV file: {e}")
        raise

    except Exception as e:
        logger.error(f"An unexpected error occurred while loading labor entries: {e}")
        raise

def syncro_prepare_ticket_labor_json(
    config,
    labor_entry: Dict[str, Any],
    ticket: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Build the request payload for creating a ticket labor (timer) entry."""

    if not ticket:
        logger.error("Ticket context is required to prepare labor entry payload.")
        return None

    ticket_number = labor_entry.get("ticket number")

    try:
        duration_raw = labor_entry.get("duration minutes")
        duration_minutes = int(float(duration_raw)) if duration_raw is not None else None
    except (TypeError, ValueError):
        logger.error(
            f"Invalid duration minutes '{labor_entry.get('duration minutes')}' for ticket {ticket_number}."
        )
        return None

    if duration_minutes is None or duration_minutes <= 0:
        logger.error(
            f"Duration minutes must be greater than zero for ticket {ticket_number}."
        )
        return None

    created_at_raw = labor_entry.get("created at")
    created_at = parse_comment_created(created_at_raw)
    if not created_at:
        logger.error(f"Invalid created-at timestamp '{created_at_raw}' for ticket {ticket_number}.")
        return None

    if created_at.tzinfo is None:
        try:
            local_timezone = pytz.timezone(SYNCRO_TIMEZONE)
            created_at = local_timezone.localize(created_at)
        except Exception as e:
            logger.error(f"Failed to localize timestamp '{created_at_raw}': {e}")
            return None

    end_at = created_at + timedelta(minutes=duration_minutes)

    tech_name = labor_entry.get("tech")
    user_id = None
    if tech_name:
        user_id_raw = get_syncro_tech(tech_name)
        if user_id_raw is not None:
            try:
                user_id = int(user_id_raw)
            except (TypeError, ValueError):
                logger.warning(f"Unable to cast tech ID '{user_id_raw}' to integer for tech '{tech_name}'.")
        else:
            logger.warning(f"Technician '{tech_name}' not found for ticket {ticket_number}.")
    else:
        logger.warning(f"No technician specified for ticket {ticket_number} labor entry.")

    product_name = labor_entry.get("labor type")
    product_id = get_syncro_product_id_by_name(product_name, config=config) if product_name else None
    if product_name and product_id is None:
        logger.warning(
            f"Labor type '{product_name}' could not be matched to a product for ticket {ticket_number}."
        )

    notes = labor_entry.get("notes")

    billable_override = parse_billable_status(labor_entry.get("billable status"))
    visibility_hidden = parse_visibility_value(labor_entry.get("visibility"))

    payload: Dict[str, Any] = {
        "start_at": created_at.isoformat(),
        "end_at": end_at.isoformat(),
        "duration_minutes": duration_minutes,
        "notes": notes,
    }

    if user_id is not None:
        payload["user_id"] = user_id
    if product_id is not None:
        payload["product_id"] = product_id
    if billable_override is not None:
        payload["billable_override"] = billable_override
    if visibility_hidden is not None:
        payload["hidden"] = visibility_hidden

    cleaned_payload = {key: value for key, value in payload.items() if value is not None}

    logger.debug(
        f"Prepared labor payload for ticket {ticket_number} (ID {ticket.get('id')}): {cleaned_payload}"
    )

    return cleaned_payload

def syncro_prepare_ticket_combined_json(config, ticket):
    """ 
    Used with the tickets_and_comments_combined.csv template

    This is the Initial ticket creation. for the combined template. for the Comment adds
    look for syncro_prepare_ticket_combined_comment_json function

    required_fields = [
        "ticket customer",
        "ticket number",
        "tech",
        "end user",
        "ticket subject",
        "ticket description",
        "timestamp",
        "email body",
        "comment owner",
        "ticket status",
        "ticket issue type",
        "ticket created date",
        "ticket priority"
    ]
    """
    # Extract individual fields
    customer = ticket.get("ticket customer")
    ticket_number = ticket.get("ticket number")
    subject = ticket.get("ticket subject")
    tech_name = ticket.get("tech")
    initial_issue = ticket.get("ticket description") or DEFAULTS.get("ticket description")
    status = ticket.get("ticket status")
    issue_type = ticket.get("ticket issue type") or DEFAULTS.get("ticket issue type")
    created = ticket.get("ticket created date")
    end_user = ticket.get("end user") or ticket.get("ticket user")
    priority = ticket.get("ticket priority")

    # Process fields
    customer_id = get_customer_id_by_name(customer, config)
    syncro_ticket_number = clean_syncro_ticket_number(ticket_number) 
    syncro_created_date = get_syncro_created_date(created)
    contact_match = get_syncro_customer_contact(customer_id, end_user)
    syncro_contact_id = contact_match.get("id") if contact_match else None
    contact_display_name = (contact_match.get("name") if contact_match else None) or tech_name or end_user
    syncro_tech = get_syncro_tech(tech_name) if tech_name else None
    initial_issue_comments = build_syncro_initial_issue(initial_issue, contact_display_name, syncro_created_date)
    syncro_issue_type = get_syncro_issue_type(issue_type)
    syncro_priority = get_syncro_priority(priority)

    # Create JSON payload
    ticket_json = {
        "customer_id": customer_id,
        "number": syncro_ticket_number,
        "subject": subject,
        "user_id": syncro_tech,
        "comments_attributes": initial_issue_comments,
        "status": status,
        "problem_type": syncro_issue_type,
        "created_at": syncro_created_date,
        "contact_id": syncro_contact_id,
        "priority": syncro_priority,
    }

    # Remove keys with None values
    ticket_json = {key: value for key, value in ticket_json.items() if value is not None}

    return ticket_json
def parse_comment_created(comment_created: Any) -> Optional[datetime]:
    """Parse a timestamp string into a ``datetime``.

    Attempts to interpret a wide variety of common date and time formats,
    respecting the configured ``TIMESTAMP_FORMAT`` for day-first or
    month-first parsing.  Examples of supported inputs include ``1-2-2025``,
    ``15-2-04``, ``05/04/25 5:04 PM`` and other similar variations.

    Args:
        comment_created: A string or ``datetime`` representing the timestamp.

    Returns:
        ``datetime`` if parsing succeeds, otherwise ``None``.
    """

    if isinstance(comment_created, datetime):
        logger.info("comment_created is already a datetime object: %s", comment_created)
        return comment_created

    if not comment_created:
        logger.error("No timestamp provided")
        return None

    try:
        parsed_date = parser.parse(
            comment_created,
            dayfirst=is_day_first(),
            fuzzy=True,
        )
        logger.info(
            "Parsed datetime using dateutil with dayfirst=%s: %s",
            is_day_first(),
            parsed_date,
        )
        return parsed_date
    except (ValueError, TypeError) as e:
        logger.warning("dateutil parser failed: %s", e)

    # Explicit format handling as fallback
    separators = ["/", "-", "."]
    possible_formats: List[str] = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]

    if is_day_first():
        base_patterns = [
            "%d{sep}%m{sep}%Y %H:%M:%S",
            "%d{sep}%m{sep}%Y %H:%M",
            "%d{sep}%m{sep}%Y %I:%M %p",
            "%d{sep}%m{sep}%y %H:%M:%S",
            "%d{sep}%m{sep}%y %H:%M",
            "%d{sep}%m{sep}%y %I:%M %p",
            "%d{sep}%m{sep}%Y",
            "%d{sep}%m{sep}%y",
        ]
    else:
        base_patterns = [
            "%m{sep}%d{sep}%Y %H:%M:%S",
            "%m{sep}%d{sep}%Y %H:%M",
            "%m{sep}%d{sep}%Y %I:%M %p",
            "%m{sep}%d{sep}%y %H:%M:%S",
            "%m{sep}%d{sep}%y %H:%M",
            "%m{sep}%d{sep}%y %I:%M %p",
            "%m{sep}%d{sep}%Y",
            "%m{sep}%d{sep}%y",
        ]

    for sep in separators:
        for pattern in base_patterns:
            possible_formats.append(pattern.format(sep=sep))

    for fmt in possible_formats:
        try:
            parsed_date = datetime.strptime(comment_created, fmt)
            logger.info("Parsed datetime using format '%s': %s", fmt, parsed_date)
            return parsed_date
        except ValueError:
            continue

    logger.error("Unrecognized date format: %s", comment_created)
    return None

def syncro_prepare_ticket_combined_comment_json(config, comment):
    """
    Used with the tickets_and_comments_combined.csv template

    This prepares a follow-up comment for the combined template once the
    initial ticket has been created.

    
    required_fields = [
        "ticket customer",
        "ticket number",
        "tech",
        "end user",
        "ticket subject",
        "ticket description",
        "timestamp",
        "email body",
        "comment owner",
        "ticket status",
        "ticket issue type",
        "ticket created date",
        "ticket priority"
    ]
    """
    #pull out individual fields and process them for creating a Syncro comment
    comment_created_raw = comment.get("timestamp")
    parsed_created = parse_comment_created(comment_created_raw)
    if parsed_created:
        comment_created = parsed_created.strftime(get_timestamp_format())
        syncro_created_date = get_syncro_created_date(comment_created)
    else:
        logger.error(f"Invalid timestamp for comment: {comment_created_raw}")
        syncro_created_date = None
    customer = comment.get("ticket customer") #need for contact lookup
    comment_owner = comment.get("comment owner")
    ticket_number = comment.get("ticket number")
    ticket_comment = comment.get("email body") or DEFAULTS.get("email body")
    comment_contact = None

    if comment_owner:
        customer_id = get_customer_id_by_name(customer, config) if customer else None

        if customer_id:
            contact_match = get_syncro_customer_contact(customer_id, comment_owner)
            if contact_match is None:
                logger.warning(
                    "Unable to resolve comment owner '%s' to a Syncro contact for customer '%s'.",
                    comment_owner,
                    customer,
                )
            else:
                comment_contact = contact_match.get("name") or comment_owner
        else:
            logger.warning(
                "Unable to determine customer id for '%s'; using comment owner name for contact.",
                customer,
            )
            comment_contact = comment_owner

        if comment_contact is None:
            comment_contact = comment_owner

    if comment_contact is None:
        comment_contact = comment.get("tech") or comment.get("end user")

    if comment_contact is None:
        logger.error(
            "No comment owner, tech, or end user available for ticket %s; comment will lack owner information.",
            ticket_number,
        )

    # Create JSON payload for a Syncro comment
    comment_json = {
        "ticket_number": ticket_number,
        "subject": "CSV - API Import",
        "created_at": syncro_created_date,
        "tech": comment_contact,
        "body": ticket_comment,
        "hidden": True,
        "do_not_email": True
    }

    # Remove keys with None values, 
    comment_json = {key: value for key, value in comment_json.items() if value is not None}

    return comment_json

def group_comments_by_ticket_number(comments):
    """
    Groups comments by ticket number.

    :param comments: List of dictionaries, each representing a ticket comment.
    :return: Dictionary where keys are ticket numbers and values are lists of comments.
    """
    
    grouped_comments = defaultdict(list)

    for comment in comments:
        #logger.info(f"Checking row Comment: {comment}")
        if isinstance(comment, tuple):  # Check if it's a tuple
            logger.warning("in group_comments_by_ticket_number: Tuple found instead of dictionary:")
            #pprint(comment)  # Print the two parts of the tuple

            if len(comment) == 2:  # If the tuple contains two parts
                key_part, value_part = comment
                logger.info(f"Key part: {key_part}")
                logger.info(f"Skipping")
            continue

        logger.info(f"Commet type is a {type(comment)}")
        logger.info(f'comment is dict and the keys are: {comment.keys()}')
        logger.info(f"Comment dict values are: {comment.values()}")

        
        ticket_number = comment.get("ticket number")
        logger.info(f"Row Ticket Number: {ticket_number}")
        #input("Press Enter to continue...")
        if ticket_number:
            logger.info(f"Adding comment to ticket number: {ticket_number}")
            grouped_comments[ticket_number].append(comment)    
    keys = grouped_comments.keys()
    logger.info(f"Keys: {keys}")
    
    return dict(grouped_comments)

def syncro_get_all_tickets_and_comments_from_combined_csv():
    
    
    logger = get_logger(__name__)

    required_fields = [
        "ticket customer",
        "ticket number",
        "tech",
        "end user",
        "comment owner",
        "ticket subject",
        "ticket description",
        "timestamp",
        "email body",
        "ticket status",
        "ticket issue type",
        "ticket created date",
        "ticket priority"
    ]

    # Ensure logger is initialized
    if logger is None:
        logger = logging.getLogger("syncro")
    
    try:
        logger.info("Attempting to load comments from CSV...")
        # ``load_csv`` relies on ``csv.DictReader`` so field order in the source
        # file or template does not affect how rows are parsed; matching header
        # names are all that is required.
        comments = load_csv(COMBINED_TICKETS_COMMENTS_CSV_PATH, required_fields=required_fields, logger=logger)
        grouped_comments_by_ticket_number = group_comments_by_ticket_number(comments)        
        logger.info(f"Successfully loaded {len(comments)} comments from {COMBINED_TICKETS_COMMENTS_CSV_PATH}.")
        #logger.info(f"Grouped comments by ticket number: {grouped_comments_by_ticket_number}")
        return grouped_comments_by_ticket_number

    except FileNotFoundError:
        logger.error(f"CSV file not found: {COMBINED_TICKETS_COMMENTS_CSV_PATH}")
        raise

    except ValueError as e:
        logger.error(f"Validation error in CSV file: {e}")
        raise

    except Exception as e:
        logger.error(f"An unexpected error occurred while loading comments: {e}")
        raise
    
def order_ticket_rows_by_date(ticket_rows_data):
    logger.info(f"Ticket Rows Data passed in is a {type(ticket_rows_data)}") 
    ticket_rows_data = ticket_rows_data.items()
    logger.info(f"Ticket Rows Data after .items() in is a {type(ticket_rows_data)}")
    ordered_ticket_rows_data = {}
    for row in ticket_rows_data:  # each row is one ticket with lots of entries, need to find the oldest entry
        ordered_entries = []  # this list should hold dict objects of each entry in the ticket
        ticket_number, ticket_data = row
        logger.info(f"starting on entries for Ticket Number: {ticket_number}")

        for ticket_entry in ticket_data:  # ticket_data is a list of all the entries in the ticket, should be all the entries in a dict object
            logger.info(f"Ticket Entry: {ticket_entry}")

            timestamp_str = ticket_entry.get("timestamp")  # I am getting the timestamp of the ticket entry
            if not timestamp_str:
                logger.warning("Missing timestamp for ticket %s entry, skipping", ticket_number)
                continue

            timestamp = parse_comment_created(timestamp_str)
            if timestamp is None:
                logger.error("Failed to parse timestamp '%s'", timestamp_str)
                continue

            ordered_entries.append((timestamp, ticket_entry))  # I am appending the timestamp and the ticket entry to the ordered_entries list

        ordered_entries.sort(key=lambda x: x[0])
        logger.info(f"ordered_entries type: {type(ordered_entries)}")  

        ordered_ticket_rows_data[ticket_number] = ordered_entries

    return ordered_ticket_rows_data

