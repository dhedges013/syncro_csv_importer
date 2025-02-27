import logging
import sys
import os
from pprint import pprint
import time
import requests

# Import from syncro_config and utils
from syncro_configs import (
    SYNCRO_API_BASE_URL,
    SYNCRO_API_KEY,
    get_logger

)


# Add parent directory to sys.path for imports
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)

# Get a logger for this module
logger = get_logger(__name__)
print(f"Handlers for {logger.name}: {logger.handlers}")
print(f"Handlers for root logger: {logging.getLogger().handlers}")

_api_call_count = 0

def increment_api_call_count():
    """Increment the global API call counter."""
    global _api_call_count
    _api_call_count += 1

def get_api_call_count() -> int:
    """Retrieve the total API call count."""
    return _api_call_count


def syncro_api_call(method: str, endpoint: str, data: dict = None, params: dict = None):
    """
    Generic API call to SyncroMSP.

    Args:
        method (str): HTTP method (e.g., 'POST', 'PUT', 'GET').
        endpoint (str): API endpoint.
        data (dict): JSON payload for the request (optional).
        params (dict): Query parameters for the request (optional).

    Returns:
        dict: JSON response from the API.
    """
    
    increment_api_call_count()
    url = f"{SYNCRO_API_BASE_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {SYNCRO_API_KEY}",
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    try:
        response = requests.request(method, url, headers=headers, json=data, params=params)
        time.sleep(0.5)
        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json() if response.content else {}
    except requests.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        raise
    except requests.RequestException as req_err:
        logger.error(f"Request error occurred: {req_err}")
        raise

def syncro_api_get(endpoint: str, params: dict = None):
    """
    Fetch paginated data from SyncroMSP API.

    Args:
        endpoint (str): The API endpoint to call (e.g., '/customers', '/contacts').
        params (dict): Query parameters for the request.

    Returns:
        list: Aggregated data from all pages.
    """
    increment_api_call_count()
    all_data = []
    if params is None:
        params = {}
    current_page = 1

    logger.info(f"Starting to fetch data from endpoint: {endpoint}")

    while True:
        params["page"] = current_page

        response = syncro_api_call("GET", endpoint, params=params)
        time.sleep(0.5)
        if not response:
            logger.error(f"Failed to fetch data from {endpoint}. Stopping pagination.")
            break

        key = endpoint.strip('/').lower()
        page_data = response.get(key, [])
        all_data.extend(page_data)

        logger.info(f"Fetched {len(page_data)} records from page {current_page}.")

        meta = response.get("meta", {})
        if not meta.get("next_page"):
            break

        current_page += 1

    logger.info(f"Finished fetching data from {endpoint}. Total records retrieved: {len(all_data)}.")
    return all_data

def syncro_get_ticket_data(ticket_id: int, key: str = None):
    """
    Fetch data for a specific ticket from SyncroMSP API.

    Args:
        ticket_id (int): The ID of the ticket to fetch.
        key (str): Optional key or nested path (e.g., 'comments', 'customer.email').

    Returns:
        dict, list, or Any: Full ticket data if no key is provided, otherwise the specific data point.
    """
    endpoint = f"/tickets/{ticket_id}"
    ticket_data = syncro_api_call("GET", endpoint)

    if ticket_data:
        ticket = ticket_data.get("ticket", {})
        logger.info(f"Retrieved data for ticket ID: {ticket_id}")
       # return extract_nested_key(ticket, key) if key else ticket
    else:
        logger.error(f"Failed to fetch data for ticket ID: {ticket_id}")
        return None

def syncro_get_all_customers():
    """Fetch all customers from SyncroMSP API and log their business_name and id."""
    customers = syncro_api_get('/customers')
    customer_info = [{"id": customer.get("id"), "business_name": customer.get("business_name")} for customer in customers]
    logger.info(f"Retrieved {len(customers)} customers: {customer_info}")
    return customers

def syncro_get_all_contacts():
    """Fetch all contacts from SyncroMSP API."""
    return syncro_api_get('/contacts')

def syncro_get_all_tickets():
    """Fetch all tickets from SyncroMSP API."""
    return syncro_api_get('/tickets')



def get_syncro_ticket_by_number(ticket_number: str) -> dict:
    """
    Retrieve a Syncro ticket by its number.

    Args:
        ticket_number (str): The number of the ticket to retrieve.

    Returns:
        dict: The ticket details if found, or None if not found or an error occurs.

    Logs:
        - Info for successful ticket retrieval.
        - Warning if no ticket is found.
        - Error if any issue occurs during execution.
    """
    endpoint = "/tickets"
    try:
        # Define the query parameter for the ticket number
        params = {"number": ticket_number}

        # Log the API request
        logger.info(f"Fetching ticket with number: {ticket_number}")

        # Call the API
        response = syncro_api_call("GET", endpoint, params=params)

        # Handle the response
        if response and "tickets" in response and len(response["tickets"]) > 0:
            ticket = response["tickets"][0]
            logger.info(f"Successfully retrieved ticket: {ticket}")
            return ticket

        # Log a warning if no ticket is found
        logger.warning(f"No ticket found with number: {ticket_number}")
        return None

    except Exception as e:
        # Log any unexpected errors
        logger.error(f"Error occurred while retrieving ticket '{ticket_number}': {e}")
        raise

def syncro_get_all_techs():
    """
    Fetch all techs (users) from SyncroMSP API.

    Returns:
        list: A list of techs with their relevant details.
    """
    try:
        # Define the endpoint for users (techs)
        endpoint = '/users'

        # Fetch data using the syncro_api_get utility
        techs = syncro_api_get(endpoint)           
        
        # Log the retrieved tech details
        logger.info(f"Retrieved {len(techs)} techs: {techs}")
        
        return techs

    except Exception as e:
        # Log any errors during the process
        logger.error(f"Error fetching techs: {e}")
        return []

def syncro_get_contacts_by_customer_id(customer_id: int) -> dict:
    """
    Fetch all contacts for a specific customer ID from the SyncroMSP API
    and build a dictionary of contact names with their corresponding IDs.

    Args:
        customer_id (int): The ID of the customer to fetch contacts for.

    Returns:
        dict: A dictionary where keys are contact names and values are contact IDs.

    Logs:
        - Info for successful retrieval of contacts.
        - Warning if no contacts are found.
        - Error if an issue occurs during the API call.
    """
    try:
        # Define the endpoint with the customer ID as a query parameter
        endpoint = '/contacts'
        params = {"customer_id": customer_id}

        # Log the API request
        logger.info(f"Fetching contacts for customer ID: {customer_id}")

        # Call the API
        contacts = syncro_api_get(endpoint, params=params)

        # Check if contacts were retrieved
        if not contacts:
            logger.warning(f"No contacts found for customer ID: {customer_id}")
            return {}

        # Build the dictionary of contact names and IDs
        contact_dict = {contact["name"]: contact["id"] for contact in contacts if "name" in contact and "id" in contact}

        # Log the built dictionary
        logger.info(f"Built contact dictionary for customer ID {customer_id}: {contact_dict}")
        return contact_dict

    except Exception as e:
        # Log any errors
        logger.error(f"Error fetching contacts for customer ID {customer_id}: {e}")
        raise

def syncro_get_issue_types() -> list:
    """
    Fetch all issue types (problem types) from the SyncroMSP settings API.

    Returns:
        list: A list of issue types (problem types).

    Logs:
        - Info for successful retrieval of issue types.
        - Warning if no issue types are found.
        - Error if an issue occurs during the API call.
    """
    try:
        # Define the endpoint for settings
        endpoint = '/settings'

        # Log the API request
        logger.info("Fetching issue types from Syncro settings")

        # Call the API
        settings = syncro_api_call("GET", endpoint)

        # Extract issue types (problem types)
        issue_types = settings.get("ticket", {}).get("problem_types", [])

        if not issue_types:
            logger.warning("No issue types found in Syncro settings.")
            return []

        # Log the retrieved issue types
        logger.info(f"Retrieved issue types: {issue_types}")
        return issue_types

    except Exception as e:
        # Log any errors
        logger.error(f"Error fetching issue types: {e}")
        raise

def syncro_get_ticket_statuses():
    """
    Fetch ticket settings from the Syncro API and update ticket statuses in syncro_temp_data.json.

    Returns:
        dict: A dictionary containing ticket statuses and other ticket settings.
    """
    endpoint = "/tickets/settings"

    try:
        # Call the Syncro API
        response = syncro_api_call("GET", endpoint)

        # Check if response contains ticket statuses
        if response and "ticket_status_list" in response:
            ticket_status_list = response["ticket_status_list"]
            logger.info(f"Retrieved ticket statuses: {ticket_status_list}")
            return ticket_status_list
        else:
            logger.error(f"Failed to retrieve ticket statuses. Response: {response}")
            return None

    except Exception as e:
        logger.error(f"Error fetching ticket settings: {e}")
        return None

if __name__ == "__main__":
    # Example usage of read functions
    #all_customers = syncro_get_all_customers()
    #pprint(all_customers)

    #all_contacts = syncro_get_all_contacts()
    #pprint(all_contacts)

    #ticket_data = syncro_get_ticket_data(ticket_id=89575281)
    #pprint(ticket_data)
    pprint(len(syncro_get_all_contacts()))
    #contacts = syncro_get_contacts_by_customer_id(30054463)
 
    
