import time
import requests

# Import from syncro_config and utils
from syncro_configs import (get_logger)

logger = get_logger(__name__)
_api_call_count = 0

def increment_api_call_count():
    """Increment the global API call counter."""
    global _api_call_count
    _api_call_count += 1

def get_api_call_count() -> int:
    """Retrieve the total API call count."""
    return _api_call_count


def syncro_api_call(config, method: str, endpoint: str, data=None, params=None) -> dict:
    """
    A generic function for all Syncro API calls (GET, POST, etc.).
    Increments the API call count, sets headers, rate-limits requests, and returns JSON.
    """
    global _api_call_count
    _api_call_count += 1

    if not params:
        params = {}
    if not data:
        data = {}

    url = f"{config.base_url}{endpoint}"
    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    try:
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data,
            params=params,
            timeout=30
        )
        # A short sleep to avoid API rate-limits
        time.sleep(0.38)

        # Raise an error if the response is 4xx or 5xx
        response.raise_for_status()

        # Return the JSON data (or an empty dict if no content)
        return response.json() if response.content else {}

    except requests.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        raise
    except requests.RequestException as req_err:
        logger.error(f"Request error occurred: {req_err}")
        raise

def syncro_api_call_paginated(config, endpoint: str, params=None) -> list:
    """
    Fetch paginated data from Syncro MSP API using the above `syncro_api_call`.
    Automatically loops through all pages until `meta["next_page"]` is not found.
    """
    if params is None:
        params = {}

    all_data = []
    current_page = 1

    logger.info(f"Starting to fetch data from {endpoint}")

    while True:
        params["page"] = current_page
        response = syncro_api_call(config, "GET", endpoint, params=params)

        if not response:
            logger.warning(f"No response or invalid response from {endpoint}, stopping pagination.")
            break

        # Syncro often returns data in a key named after the endpoint, e.g. "tickets"
        key = endpoint.strip("/").lower()
        page_data = response.get(key, [])

        all_data.extend(page_data)
        logger.info(f"Fetched {len(page_data)} records from page {current_page}.")

        meta = response.get("meta", {})
        if not meta.get("next_page"):
            break  # No more pages

        current_page += 1

    logger.info(f"Finished fetching data from {endpoint}, total records: {len(all_data)}.")
    return all_data

def syncro_get_all_customers(config):
    """Fetch all customers from SyncroMSP API and log their business_name and id."""    
    endpoint = '/customers'
    try:
        customers = syncro_api_call_paginated(config, endpoint)
        customer_info = [{"id": customer.get("id"), "business_name": customer.get("business_name")} for customer in customers]
        logger.info(f"Retrieved {len(customers)} customers: {customer_info}")       
        return customers
    except Exception as e:
        logger.error(f"Error fetching customers: {e}")
        return []

def syncro_get_all_contacts(config):
    """Fetch all contacts from SyncroMSP API."""
    endpoint = '/contacts'
    try:        
        contacts = syncro_api_call_paginated(config, endpoint)
        logger.info(f"Retrieved {len(contacts)} contacts: {contacts}")
        return contacts
    except Exception as e:
        logger.error(f"Error fetching contacts: {e}")
        return []
 
def syncro_get_all_tickets(config):
    endpoint = '/tickets'
    try:        
        tickets = syncro_api_call_paginated(config, endpoint)
        logger.info(f"Retrieved {len(tickets)} tickets: {tickets}")
        return tickets
    except Exception as e:
        logger.error(f"Error fetching tickets: {e}")
        return []

def syncro_get_all_techs(config):
    """Fetch all techs (users) from SyncroMSP API."""
    endpoint = '/users'
    try:   
        techs = syncro_api_call_paginated(config, endpoint)      
        logger.info(f"Retrieved {len(techs)} techs: {techs}")        
        return techs

    except Exception as e:
        logger.error(f"Error fetching techs: {e}")
        return []


def syncro_get_ticket_data(config, ticket_id: int):
    """Fetch data for a single ticket."""
    endpoint = f"/tickets/{ticket_id}"
    try:
        ticket_data = syncro_api_call(config, "GET", endpoint)
        ticket = ticket_data.get("ticket", {})
        logger.info(f"Retrieved data for ticket {ticket_id}")
        return ticket
    except Exception as e:
        logger.error(f"Error retrieving ticket {ticket_id}: {e}")
        return None

def get_syncro_ticket_by_number(ticket_number: str,config) -> dict:
    """Retrieve a Syncro ticket by its number. """
    endpoint = "/tickets"
    try:
        # Define the query parameter for the ticket number
        params = {"number": ticket_number}
        logger.info(f"Fetching ticket with number: {ticket_number}")
        response = syncro_api_call(config,"GET", endpoint,params=params)

        # Handle the response
        if response and "tickets" in response and len(response["tickets"]) > 0:
            ticket = response["tickets"][0]
            logger.info(f"Successfully retrieved ticket: {ticket}")
            return ticket
        logger.warning(f"No ticket found with number: {ticket_number}")
        return None

    except Exception as e:
        logger.error(f"Error occurred while retrieving ticket '{ticket_number}': {e}")
        raise

def syncro_get_contacts_by_customer_id(customer_id: int,config) -> dict:
    """Fetch all contacts for a specific customer ID from the SyncroMSP API"""
    endpoint = '/contacts'
    try:        
        params = {"customer_id": customer_id}
        logger.info(f"Fetching contacts for customer ID: {customer_id}")
        contacts = syncro_api_call(config, "GET", endpoint, params=params)   
        #contacts = syncro_api_get(endpoint,data=None, params=params,config)

        # Check if contacts were retrieved
        if not contacts:
            logger.warning(f"No contacts found for customer ID: {customer_id}")
            return {}

        # Build the dictionary of contact names and IDs
        contact_dict = {contact["name"]: contact["id"] for contact in contacts if "name" in contact and "id" in contact}
        logger.info(f"Built contact dictionary for customer ID {customer_id}: {contact_dict}")
        return contact_dict

    except Exception as e:
        # Log any errors
        logger.error(f"Error fetching contacts for customer ID {customer_id}: {e}")
        raise

def syncro_get_issue_types(config) -> list:
    """Fetch all issue types (problem types) from the SyncroMSP settings API. """
    endpoint = '/settings'
    try:        
        logger.info("Fetching issue types from Syncro settings")
        settings = syncro_api_call(config,"GET", endpoint)
        issue_types = settings.get("ticket", {}).get("problem_types", [])

        if not issue_types:
            logger.warning("No issue types found in Syncro settings.")
            return []
        logger.info(f"Retrieved issue types: {issue_types}")
        return issue_types

    except Exception as e:
        # Log any errors
        logger.error(f"Error fetching issue types: {e}")
        raise

def syncro_get_ticket_statuses(config):
    """
    Fetch ticket settings from the Syncro API and update ticket statuses in syncro_temp_data.json.

    Returns:
        dict: A dictionary containing ticket statuses and other ticket settings.
    """
    endpoint = "/tickets/settings"

    try:
        # Call the Syncro API
        response = syncro_api_call(config,"GET", endpoint)

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
    print("This module is not meant to be executed")

 
    
