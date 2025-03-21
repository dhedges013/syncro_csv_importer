import os
import sys
from datetime import datetime
from dateutil import parser
import json
import logging
from typing import Any, Dict, List
import csv
import pytz
from collections import defaultdict

from syncro_configs import (
    get_logger,
    TEMP_FILE_PATH,
    SYNCRO_TIMEZONE,
    TICKETS_CSV_PATH,
    COMMENTS_CSV_PATH,
    COMBINED_TICKETS_COMMENTS_CSV_PATH
)

from syncro_read import (
    syncro_get_all_techs,
    syncro_get_issue_types,
    syncro_get_all_customers,
    syncro_get_all_contacts,
    syncro_get_ticket_statuses    
)

_temp_data_cache = None  # Global cache for temp data

# Get a logger for this module
logger = get_logger(__name__)

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
                _temp_data_cache = json.load(file)
                return _temp_data_cache
        except Exception as e:
            logger.error(f"Failed to load temp data from file: {e}")

    # Fetch data from Syncro API 
    logger.info("Fetching data from Syncro API...")
    try:
        techs = syncro_get_all_techs(config)
        issue_types = syncro_get_issue_types(config)
        customers = syncro_get_all_customers(config)
        contacts = syncro_get_all_contacts(config)
        statuses = syncro_get_ticket_statuses(config)

        _temp_data_cache = {
            "techs": techs,
            "issue_types": issue_types,
            "customers": customers,
            "contacts": contacts,
            "statuses": statuses
        }

        # Save to temp file
        logger.info(f"Saving temp data to {TEMP_FILE_PATH}")
        with open(TEMP_FILE_PATH, "w") as file:
            json.dump(_temp_data_cache, file)

    except Exception as e:
        logger.error(f"Failed to fetch data from Syncro API or save temp data: {e}")
        raise

    return _temp_data_cache

# Add parent directory to sys.path for imports
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)

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
            headers = reader.fieldnames

            if required_fields:
                missing_fields = [field for field in required_fields if field not in headers]
                if missing_fields:
                    raise ValueError(f"Missing required fields in CSV file: {missing_fields}")

            data = []
            for row_number, row in enumerate(reader, start=1):
                cleaned_row = {}
                for key, value in row.items():
                    if value is None or value.strip() == "":
                        raise ValueError(f"Row {row_number}: Empty value found in field '{key}'.")
                    cleaned_row[key] = value

                if required_fields:
                    for field in required_fields:
                        if field not in cleaned_row or cleaned_row[field].strip() == "":
                            raise ValueError(f"Row {row_number}: Missing or blank required field '{field}'.")

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
        logger.error(f"Error reading CSV file {filepath}: {e}")
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

def syncro_get_all_tickets_from_csv(config: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Load all tickets from a CSV file and validate them against cached data.
    """
    required_fields = [
        "ticket customer",
        "ticket number",
        "ticket subject",
        "tech",
        "ticket initial issue",
        "ticket status",
        "ticket issue type",
        "ticket created"
    ]

    try:
        logger.info("Checking and Creating _temp_data_cache from API...")
        temp_data = load_or_fetch_temp_data(config)

        logger.info("Attempting to load tickets from CSV...")
        tickets = load_csv(TICKETS_CSV_PATH, required_fields=required_fields, logger=logger)

        # Validate data
        validate_ticket_data(tickets, temp_data, logger)

        logger.debug(f"Successfully loaded {len(tickets)} tickets from {TICKETS_CSV_PATH}.")
        return tickets

    except FileNotFoundError:
        logger.error(f"CSV file not found: {TICKETS_CSV_PATH}")
        raise
    except ValueError as e:
        logger.error(f"Validation error in CSV file: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading tickets: {e}")
        raise


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

def build_syncro_initial_issue(initial_issue: str, syncroContact: str) -> list:
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
                "subject": "CSV Import",
                "body": initial_issue,
                "hidden": True,
                "do_not_email": True,
                "tech": syncroContact  }
              
            
        
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

        formats = [
            "%Y-%m-%d",          # Date only (YYYY-MM-DD)
            "%m/%d/%Y",          # MM/DD/YYYY
            "%d-%m-%Y",          # DD-MM-YYYY
            "%Y-%m-%d %H:%M:%S", # Full datetime (YYYY-MM-DD HH:MM:SS)
            "%Y/%m/%d %H:%M",    # Datetime without seconds (YYYY/MM/DD HH:MM)
            "%m/%d/%Y %H:%M",    # MM/DD/YYYY HH:MM (24-hour format)
            "%m/%d/%Y %I:%M %p", # MM/DD/YYYY HH:MM AM/PM (12-hour format with AM/PM)
            "%m/%d/%y %H:%M",    # MM/DD/YY HH:MM (2-digit year, 24-hour format) ✅
            "%m-%d-%y",          # MM-DD-YY
            "%Y-%m-%dT%H:%M:%S"  # ISO 8601 without timezone
        ]
        # Attempt to parse with provided formats
        parsed_date = None
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(created, fmt)
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

def get_syncro_customer_contact(customerid: str, contact: str):
    """
    Find the closest matching contact ID for a contact name within a specific customer.

    Args:
        customerid (str): The ID of the customer.
        contact (str): The name of the contact to find.

    Returns:
        int: The ID of the closest matching contact, or None if no match is found.

    Logs:
        - Info for customer and contact searches.
        - Warning if no matching contact is found.
        - Info on the closest match and its similarity score.
        - Error if any issue occurs during execution.
    """
    try:
        # Validate inputs
        if not contact:
            logger.warning(f"Contact name is missing or None for customer ID: {customerid}")
            return None

        # Load temp data
        temp_data = load_or_fetch_temp_data()
        contacts_data = temp_data.get("contacts", [])

        # Log the search process
        logger.debug(f"Looking up customer ID for customer: {customerid}")

        if not customerid:
            logger.error(f"For Contact Lookup you need the Customer ID. Customer '{customerid}' not found.")
            return None

        # Filter contacts for the given customer ID
        customer_contacts = [
            c for c in contacts_data if c.get("customer_id") == customerid
        ]

        if not customer_contacts:
            logger.warning(f"No contacts found for customer ID: {customerid}")
            return None

        # Normalize the input contact name and contact keys
        normalized_input_contact = contact.strip().lower()
        normalized_filtered_contacts = {
            c["name"].strip().lower(): c
            for c in customer_contacts
            if c.get("name") and c.get("id")
        }
        
        logger.debug(f"number of contacts found was {len(normalized_filtered_contacts)}")
        logger.debug(f"Contact Lookup: Normalized input contact: {normalized_input_contact}")
        #logger.debug(f"Contact Lookup: Normalized filtered contacts: {normalized_filtered_contacts}") # print all contacts which can be a long list in the log file

        # Check for an exact match
        if normalized_input_contact in normalized_filtered_contacts:
            logger.debug(f"Match found for contact '{contact}' in customer ID: {customerid}")           
            found_contact_id = normalized_filtered_contacts[normalized_input_contact].get("id")

            return found_contact_id
        
        # Log no match found
        logger.warning(f"No exact found for contact '{contact}' in customer ID: {customerid}")
        return None

    except Exception as e:
        logger.error(f"Error occurred while finding contact '{contact}' for customer ID: {customerid}: {e}")
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
            logger.warning("No issue types found in Syncro settings. Returning Other")
            syncro_issue_type = "Other"
            return syncro_issue_type

        # Normalize the input for case-insensitive comparison
        normalized_issue_type = issue_type.strip().lower()

        # Search for a match in the retrieved issue types
        for syncro_issue_type in issue_types:
            if syncro_issue_type.strip().lower() == normalized_issue_type:
                logger.debug(f"Match found: Input '{issue_type}' matches Syncro issue type '{syncro_issue_type}'.")
                return syncro_issue_type

        # Log a warning if no match is found
        logger.warning(f"No match found for issue type: {issue_type}")
        return None

    except KeyError as e:
        logger.error(f"Key error while accessing issue types: {e}")
        return None

    except Exception as e:
        logger.error(f"Error occurred while matching issue type '{issue_type}': {e}")
        return None

def syncro_get_all_comments_from_csv() -> List[Dict[str, Any]]:
    """
    Load all comments from a CSV file.

    Args:
        logger (logging.Logger, optional): Logger instance for logging.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents a ticket.

    Raises:
        Exception: If loading comments fails for any reason.
    """  
    required_fields = [        
        "ticket number",                         
        "ticket comment",
        "comment contact", 
        "comment created"
    ]

    try:
        logger.info("Attempting to load comments from CSV...")
        comments = load_csv(COMMENTS_CSV_PATH, required_fields=required_fields, logger=logger)
        logger.info(f"Successfully loaded {len(comments)} comments from {COMMENTS_CSV_PATH}.")
        return comments

    except FileNotFoundError:
        logger.error(f"CSV file not found: {COMMENTS_CSV_PATH}")
        raise

    except ValueError as e:
        logger.error(f"Validation error in CSV file: {e}")
        raise

    except Exception as e:
        logger.error(f"An unexpected error occurred while loading comments: {e}")
        raise

def syncro_prepare_ticket_combined_json(ticket):
    """ 
    Used with the tickets_and_comments_combined.csv template

    This is the Initial ticket creation. for the combined template. for the Comment adds
    look for syncro_prepare_ticket_combined_comment_json function

    required_fields = [
        "ticket customer",
        "ticket number",  
        "user",              
        "ticket subject",
        "ticket description",
        "ticket response", 
        "timestamp",
        "email body",
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
    #Missing Tech
    initial_issue = ticket.get("ticket description")
    status = ticket.get("ticket status")
    issue_type = ticket.get("ticket issue type")
    created = ticket.get("ticket created date")
    contact = ticket.get("ticket user")
    priority = ticket.get("ticket priority")

    # Process fields
    customer_id = get_customer_id_by_name(customer)
    syncro_ticket_number = clean_syncro_ticket_number(ticket_number) 
    #Missing processing Tech data   
    syncro_created_date = parse_comment_created(created)
    syncro_contact = get_syncro_customer_contact(customer_id, contact)
    initial_issue_comments = build_syncro_initial_issue(initial_issue, contact)
    syncro_issue_type = get_syncro_issue_type(issue_type)
    syncro_priority = get_syncro_priority(priority)

    # Create JSON payload
    ticket_json = {
        "customer_id": customer_id,
        "number": syncro_ticket_number,
        "subject": subject,
        #"user_id": syncro_tech,        
        "comments_attributes": initial_issue_comments,
        "status": status,
        "problem_type": syncro_issue_type,
        "created_at": syncro_created_date,
        "contact_id": syncro_contact,
        "priority": syncro_priority,
    }

    # Remove keys with None values
    ticket_json = {key: value for key, value in ticket_json.items() if value is not None}

    return ticket_json
def parse_comment_created(comment_created):
    """
    Parses the comment timestamp to ensure it is correctly formatted.

    - Uses `dateutil.parser.parse` to automatically handle most formats.
    - If `dateutil.parser` fails, falls back to explicit parsing.
    - Handles both MM/DD/YYYY and DD/MM/YYYY by attempting logical swaps.
    """

    if isinstance(comment_created, datetime):
        logger.info(f"comment_created is already a datetime object: {comment_created}")
        return comment_created  # Return as is
    try:
        # Try using dateutil's parser for automatic parsing
        parsed_date = parser.parse(comment_created, dayfirst=False)
        logger.info(f"Parsed datetime using dateutil: {parsed_date}")
        return parsed_date

    except (ValueError, TypeError) as e:
        logger.warning(f"dateutil parser failed: {e}")

    # Explicit format handling as fallback
    possible_formats = [
        "%m/%d/%Y %I:%M %p",  # MM/DD/YYYY 12-hour
        "%m/%d/%Y %H:%M",     # MM/DD/YYYY 24-hour
        "%m/%d/%y %H:%M",     # MM/DD/YY 24-hour
        "%d/%m/%Y %H:%M",     # DD/MM/YYYY 24-hour
        "%d/%m/%y %H:%M",     # DD/MM/YY 24-hour
        "%Y-%m-%d %H:%M:%S",  # ISO format
    ]

    for fmt in possible_formats:
        try:
            parsed_date = datetime.strptime(comment_created, fmt)
            logger.info(f"Parsed datetime using format '{fmt}': {parsed_date}")
            return parsed_date
        except ValueError:
            continue

    # Last attempt: flip month and day if format is ambiguous
    try:
        temp_date = datetime.strptime(comment_created, "%m/%d/%y %H:%M")
        flipped_date = datetime(temp_date.year, temp_date.day, temp_date.month, temp_date.hour, temp_date.minute)
        logger.info(f"Converted timestamp (flipped month/day): {flipped_date}")
        return flipped_date
    except ValueError as e:
        logger.error(f"Final attempt failed: {e}")

    logger.error(f"Unrecognized date format: {comment_created}")
    return None

def syncro_prepare_ticket_combined_comment_json(comment):
    """
    Used with the tickets_and_comments_combined.csv template

    This is the comment creation. for the combined template. for the the intial ticket creation
    look for syncro_prepare_ticket_json function

    
    required_fields = [
        "ticket customer",
        "ticket number",  
        "user",              
        "ticket subject",
        "ticket description",
        "ticket response", 
        "timestamp",
        "email body",
        "ticket status",
        "ticket issue type",
        "ticket created date",
        "ticket priority"
    ]
    """ 
    #pull out individual fields and process them for creating a Syncro comment
    comment_created =  comment.get("timestamp") 
    comment_created = parse_comment_created(comment_created)
    comment_created = comment_created.strftime("%m/%d/%y %H:%M")
    customer = comment.get("ticket customer") #need for contact lookup
    ticket_number = comment.get("ticket number") 
    ticket_comment = comment.get("email body")           
    comment_contact = comment.get("user")  

    # Process fields    
    syncro_created_date = get_syncro_created_date(comment_created)

    # Create JSON payload for a Syncro comment
    comment_json = {
        "ticket_number": ticket_number,
        "subject": "API Import",
        "created_at": syncro_created_date,
        "tech": comment_contact,
        "body": ticket_comment,
        "hidden": True,
        "do_not_email": True
    }

    # Remove keys with None values, 
    comment_json = {key: value for key, value in comment_json.items() if value is not None}

    return comment_json
def syncro_prepare_ticket_json(config,ticket):
    """
    Extract ticket data into variables and create a JSON package for Syncro ticket creation.
    Removes fields with None values.

    Args:
        ticket (dict): Ticket data dictionary.

    Returns:
        dict: JSON payload for Syncro ticket creation.
    """
    # Extract individual fields
    customer = ticket.get("ticket customer")
    ticket_number = ticket.get("ticket number")
    subject = ticket.get("ticket subject")
    tech = ticket.get("tech")
    initial_issue = ticket.get("ticket initial issue")
    status = ticket.get("ticket status")
    issue_type = ticket.get("ticket issue type")
    created = ticket.get("ticket created")
    contact = ticket.get("ticket contact")
    priority = ticket.get("ticket priority")

    # Process fields
    customer_id = get_customer_id_by_name(customer,config)  #function is in syncro_reads
    syncro_ticket_number = clean_syncro_ticket_number(ticket_number) #function is in syncro_utils
    syncro_tech = get_syncro_tech(tech) #function is in syncro_utils
    syncro_created_date = get_syncro_created_date(created) #function is in syncro_utils
    syncro_contact = get_syncro_customer_contact(customer_id, contact) #function is in syncro_utils
    initial_issue_comments = build_syncro_initial_issue(initial_issue, contact) #function is in syncro_utils
    syncro_issue_type = get_syncro_issue_type(issue_type) #function is in syncro_utils
    syncro_priority = get_syncro_priority(priority) #function is in syncro_utils

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
        "contact_id": syncro_contact,
        "priority": syncro_priority,
    }

    # Remove keys with None values
    ticket_json = {key: value for key, value in ticket_json.items() if value is not None}
    return ticket_json

def syncro_prepare_comments_json(comment):
    """
    Extract ticket data into variables and create a JSON package for Syncro ticket creation.
    Removes fields with None values.

    Args:
        ticket (dict): Ticket data dictionary.

    Returns:
        dict: JSON payload for Syncro ticket creation.
    """    
    required_fields = [        
        "ticket number", 
        "comment subject",                       
        "ticket comment",
        "comment contact",
        "comment created"
    ]

    # Extract individual fields 
    ticket_number = comment.get("ticket number") 
    comment_subject = comment.get("comment subject")
    ticket_comment = comment.get("ticket comment") # I tried but it didnt work
    comment_created =  comment.get("comment created")       
    comment_contact = comment.get("comment contact") # System, Daniel Hedges, Sally Joe

    # Process fields
    syncro_created_date = parse_comment_created(comment_created)

    # Create JSON payload
    comment_json = {
        "ticket_number": ticket_number,
        "subject": comment_subject,
        "created_at": syncro_created_date,
        "tech": comment_contact,
        "body": ticket_comment,
        "hidden": True,
        "do_not_email": True
    }

    # Remove keys with None values
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
        "user",              
        "ticket subject",
        "ticket description",
        "ticket response", 
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
    for row in ticket_rows_data: # each row is one ticket with lots of entries, need to find the oldest entry
        ordered_entries = [] #this list should hold dict objects of each entry in the ticket
        ticket_number, ticket_data = row
        logger.info(f"starting on entries for Ticket Number: {ticket_number}")
        #logger.info(f"ticket_data: is a {type(ticket_data)}")

        for ticket_entry in ticket_data: # ticket_data is a list of all the entries in the ticket, should be all the entries in a dict object
            logger.info(f"Ticket Entry: {ticket_entry}")
            #logger.info(F"Ticket Entry Type: {type(ticket_entry)}")                  

            timestamp = ticket_entry.get("timestamp") # I am getting the timestamp of the ticket entry
            if "AM" in timestamp or "PM" in timestamp: # Since the time is not in the same format as the other entries, I need to convert it to the same format
                logger.info(f"Timestamp contains AM or PM: {timestamp}")
                
                try:
                    timestamp = datetime.strptime(timestamp, "%m/%d/%Y %I:%M %p")
                except ValueError as e:
                    logger.info(f"ValueError: {e}")
            else:
                logger.info(f"Timestamp does not contain AM or PM: {timestamp}")
                logger.info(f"Converting timestamp to %m%d%Y datetime: {timestamp}") 
                timestamp = datetime.strptime(timestamp, "%m/%d/%y %H:%M")
                logger.info(f"Converted timestamp: {timestamp}")

            ordered_entries.append((timestamp, ticket_entry)) # I am appending the timestamp and the ticket entry to the ordered_entries list

        ordered_entries.sort(key=lambda x: x[0])
        logger.info(f"ordered_entries type: {type(ordered_entries)}")  

        ordered_ticket_rows_data[ticket_number] = ordered_entries

    return ordered_ticket_rows_data

def syncro_prepare_row_json(row):
    pass


if __name__ == "__main__":
    
    comment_created = "6/12/2024 15:00"
    date = parse_comment_created(comment_created)
    print(date)
    
    tickets = syncro_get_all_tickets_and_comments_from_combined_csv()    
    tickets_in_order = order_ticket_rows_by_date(tickets)
    logger.info(f"Tickets in order type: {type(tickets_in_order)}")
    for key, value in tickets_in_order.items():
        logger.info(f"Key: {key}, Value Type: {type(value)}")
