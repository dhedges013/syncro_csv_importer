from syncro_utils import  syncro_get_all_tickets_from_csv, syncro_prepare_ticket_json
from syncro_write import syncro_create_ticket
from syncro_read import get_api_call_count
from syncro_configs import get_logger


logger = get_logger(__name__)
def run_tickets(config):
    try:
        tickets = syncro_get_all_tickets_from_csv(logger,config)        
        logger.info(f"Loaded tickets: {len(tickets)}")
    except Exception as e:
        logger.critical(f"Failed to load tickets: {e}")

    for ticket in tickets:
        ticket_json = syncro_prepare_ticket_json(ticket,config)
        logger.info(f"Attempting to create Ticket: {ticket_json}")

        syncro_create_ticket(ticket_json,config)
    api_call_count = get_api_call_count()
    logger.info(f"Total API calls made during program run: {api_call_count}")
        

if __name__ == "__main__":
    print("This is main_tickets.py")
