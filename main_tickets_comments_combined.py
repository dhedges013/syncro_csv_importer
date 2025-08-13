
from csv_utils import syncro_get_all_tickets_and_comments_from_combined_csv
from syncro_utils import (
    order_ticket_rows_by_date,
    syncro_prepare_ticket_combined_comment_json,
    syncro_prepare_ticket_combined_json,
)

from syncro_write import (
    syncro_create_ticket,
    syncro_create_comment
)

from syncro_read import get_syncro_ticket_by_number
from syncro_configs import get_logger


logger = get_logger(__name__)


def run_tickets_comments_combined(config):
    try:
        tickets = syncro_get_all_tickets_and_comments_from_combined_csv()
        tickets_in_order = order_ticket_rows_by_date(tickets)
    except Exception as e:
        logger.critical(f"Failed to load combined tickets and comments: {e}")
        return

    for ticket_number, entries in tickets_in_order.items():
        existing_ticket = get_syncro_ticket_by_number(config, ticket_number)

        if existing_ticket:
            logger.info(f"Ticket {ticket_number} does exist, passing")
            continue

        logger.info(f"Ticket {ticket_number} does not exist, creating ticket")
        for index, (timestamp, ticket_data) in enumerate(entries):
            if index == 0:
                json_payload = syncro_prepare_ticket_combined_json(config, ticket_data)
                logger.info(f"Creating new ticket: {ticket_number}")
                syncro_create_ticket(config, json_payload)
            else:
                json_payload = syncro_prepare_ticket_combined_comment_json(ticket_data)
                logger.info(f"Adding comment to ticket: {ticket_number}")
                syncro_create_comment(config, json_payload)

        logger.info(f"Completed Ticket {ticket_number}")


if __name__ == "__main__":
    print("This is main_tickets_comments_combined.py")
