from syncro_utils import  syncro_get_all_tickets_from_csv, syncro_prepare_ticket_json
from syncro_write import syncro_create_ticket
from syncro_read import get_api_call_count
from syncro_configs import get_logger

def main():
    logger = get_logger("main")

    try:
        tickets = syncro_get_all_tickets_from_csv(logger)  # Function now properly imported
        
        print(f"Loaded tickets: {len(tickets)}")
    except Exception as e:
        logger.critical(f"Failed to load tickets: {e}")

    for ticket in tickets:
        ticket_json = syncro_prepare_ticket_json(ticket)        
        logger.info(f"Attempting to create Ticket: {ticket_json}")
        syncro_create_ticket(ticket_json)
    api_call_count = get_api_call_count()
    logger.info(f"Total API calls made during program run: {api_call_count}")
        

if __name__ == "__main__":
    main()
