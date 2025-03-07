import os
from datetime import datetime
import sys
import requests
from syncro_utils import check_duplicate_customer, check_duplicate_contact
from syncro_configs import get_logger
from syncro_read import syncro_get_all_contacts,syncro_api_call, get_syncro_ticket_by_number

from syncro_logging_configs import main_logger, ticket_creation_error_logger

# Add parent directory to sys.path for imports
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)

# Get a logger for this module
logger = get_logger(__name__)

def syncro_create_customer(config,customer_data: dict):
    """
    Create a new customer in SyncroMSP.

    Args:
        customer_data (dict): Dictionary containing customer details (e.g., name, email, phone, address).

    Returns:
        dict: Response data from the API, or None if an error occurs.
    """
    endpoint = "/customers"
    
    # Get existing customers and check for duplicates

    new_customer = {"business_name": customer_data.get("business_name")}
    customer_name = list(new_customer.values())[0]

    duplicate = check_duplicate_customer(customer_name,config)
    
    if duplicate:
        logger.warning(f"Duplicate customer found: {duplicate}")
        return None
    
    # Create new customer if no duplicate found
    response = syncro_api_call(config,"POST", endpoint, data=customer_data)    
    if response:
        logger.info(f"Successfully created customer: {response.get('customer', {}).get('name', 'Unknown')}")
    else:
        logger.error("Failed to create customer.")
    return response

def syncro_create_contact(contact_data: dict):
    """
    Create a new contact in SyncroMSP.

    Args:
        contact_data (dict): Dictionary containing contact details (e.g., first_name, last_name, email, customer_id).

    Returns:
        dict: Response data from the API, or None if an error occurs.
    """
    endpoint = "/contacts"
    existing_contacts = syncro_get_all_contacts()
    duplicate = check_duplicate_contact(existing_contacts, contact_data, "email", customer_id=contact_data.get("customer_id"))
    if duplicate:
        logger.warning(f"Duplicate contact found under customer ID {contact_data.get('customer_id')}: {duplicate}")
        return None
    response = syncro_api_call("POST", endpoint, data=contact_data)
    if response:
        logger.info(f"Successfully created contact: {response.get('contact', {}).get('first_name', 'Unknown')} {response.get('contact', {}).get('last_name', '')}")
    else:
        logger.error("Failed to create contact.")
    return response

def syncro_create_ticket(ticket_data: dict,config) -> dict:
    """
    Create a new ticket in SyncroMSP using the specified fields.

    Args:
        ticket_data (dict): Dictionary containing ticket details.

    Returns:
        dict: Response data from the API, or None if an error occurs.
    """    

    endpoint = "/tickets"
    try:
        # Extract the ticket number from the payload
        ticket_number = ticket_data.get("number")
        if not ticket_number:
            logger.error("Ticket number is missing from the payload. Next Available Ticket number will be used") 
            ticket_number = None          
        else:
            # Check if the ticket number already exists
            existing_ticket = get_syncro_ticket_by_number(ticket_number,config)
            if existing_ticket:
                logger.warning(f"Ticket number '{ticket_number}' already taken. Skipping ticket creation.")                
                ticket_creation_error_logger.error(f"ERROR: {ticket_data}")
                return None
            else:
                logger.info(f"Ticket number '{ticket_number}' is available.")         
            
        # Prepare the ticket payload using the provided fields
        payload = ticket_data
        comments_attributes = payload.get("comments_attributes", [])
        
        logger.info(f"Creating a ticket with payload: {payload}")  
        try:
            response = syncro_api_call(config,"POST", endpoint,params=payload)

            if response:
                ticket_number =response.get('ticket', {}).get('number', 'Unknown')

                logger.info(f"Successfully created ticket: {ticket_number}")
                logger.info(f"creating intial issue {comments_attributes}")
                try:

                    logger.info(f"adding ticket number to intial issue ")
                    comments_attributes[0]["ticket_number"] = ticket_number
                    comment_response = syncro_create_comment(config, comments_attributes)
                    if comment_response:
                        logger.info(f"Successfully created comment for ticket: {ticket_number}")
                    else:
                        logger.error("Failed to create comment.")
                except Exception as e:
                    logger.error(f"Error creating comment for ticket: {ticket_number}: {e}")
            else:
                logger.error("Failed to create ticket.")
            return response
        except Exception as e:
            logger.error(f"Error creating ticket: {e}")
            return None

    except requests.exceptions.HTTPError as http_err:
        # Log HTTP error details
        logger.error(f"HTTP error occurred: {http_err}")
        if hasattr(http_err, 'response') and http_err.response is not None:
            logger.error(f"Response content: {http_err.response.text}")
        return None

    except Exception as e:
        # Log unexpected errors
        logger.error(f"Unexpected error occurred while creating ticket: {e}")
        return None


def syncro_create_comment(comment_data: dict,config) -> dict:
    """
    Create a new comment in SyncroMSP using the specified fields.

    Args:
        comment_data (dict): Dictionary containing comment details.

    Returns:
        dict: Response data from the API, or None if an error occurs.
    """
    try:       
        if type(comment_data) == list:
            logger.info(f"Creating Comment Function: {len(comment_data)} comments.")
            comment_data = comment_data[0]
        else:
            logger.info("Creating Comment Function:Comment data is not a list, Creating a single comment.")
            comment_data = comment_data

        ticket_number = comment_data.get("ticket_number")

        if not ticket_number:
            logger.error("Ticket number is missing from the payload.")
            return None

        # Check if the ticket number already exists
        existing_ticket = get_syncro_ticket_by_number(ticket_number,config)
        if existing_ticket is None:
            logger.warning(f"Creating Comment Function: Ticket number '{ticket_number}' is not found. Skipping comment creation.")
            return None

        # Extract ticket ID
        ticket_id = existing_ticket.get("id")
        if not ticket_id:
            logger.error(f"Failed to retrieve ticket ID for ticket number '{ticket_number}'.")
            return None
        
        #Logic to look for existing comments
        existing_comments = existing_ticket.get("comments", [])
        if existing_comments is not None:
            logger.info(f"Creating Comment Function: Found Existing {len(existing_comments)} comments for ticket number '{ticket_number}' ")
            for comment in existing_comments:
                logger.info(f"Creating Comment Function: Comparing existing comment body: {comment.get('body')} with new comment body: {comment_data.get('body')}")
                if comment.get("body") == comment_data.get("body"):
                    logger.warning(f"Creating Comment Function: Comment already exists for ticket number '{ticket_number}'. Skipping comment creation.")
                    return None  # Move to the next comment 
        else:
            logger.info(f"Creating Comment Function: No existing comments for ticket number '{ticket_number}'.")
            

        endpoint = f"/tickets/{ticket_id}/comment"
        
        
        # Prepare the ticket payload using the provided fields
        #payload = comment_data
        # Convert datetime objects to strings in ISO format
        payload = {key: (value.isoformat() if isinstance(value, datetime) else value) for key, value in comment_data.items()}


        # Log the prepared payload
        logger.info(f"Creating a comment with payload: {payload}")
        #response = syncro_api_call("POST", endpoint, data=payload)
        response = syncro_api_call(config,"POST", endpoint,data=payload)
        
        # Handle the response
        if response and "error" not in response:
            logger.info(f"Successfully created ticket: {response.get('ticket', {}).get('number', 'Unknown')}")
            return response
        else:
            logger.error(f"Failed to create ticket. Response: {response}")
            return None
        
    except requests.exceptions.HTTPError as http_err:
        # Log HTTP error details
        logger.error(f"HTTP error occurred: {http_err}")
        if hasattr(http_err, 'response') and http_err.response is not None:
            logger.error(f"Response content: {http_err.response.text}")
        return None

    except Exception as e:
        # Log unexpected errors
        logger.error(f"Unexpected error occurred while creating ticket: {e}")
        return None



if __name__ == "__main__":
    print("This module is not intended to be run directly.")
