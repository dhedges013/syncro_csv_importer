
from syncro_utils import (
    syncro_get_all_tickets_and_comments_from_combined_csv,
    order_ticket_rows_by_date,
    syncro_prepare_ticket_combined_comment_json,
    syncro_prepare_ticket_combined_json
)

from syncro_write import (
    syncro_create_ticket,
    syncro_create_comment
)

from syncro_read import get_syncro_ticket_by_number
from syncro_configs import get_logger

def main():
    logger = get_logger("main")    
    tickets = syncro_get_all_tickets_and_comments_from_combined_csv()
    
    tickets_in_order = order_ticket_rows_by_date(tickets)
    logger.info(f"Tickets in order type: {type(tickets_in_order)}")
    for key, value in tickets_in_order.items():
        logger.info(f"Key: {key}, Value Type: {type(value)}")
        
        ticket_number= key
        check_for_existing_tickets = get_syncro_ticket_by_number(ticket_number)     
        
        if check_for_existing_tickets:
            logger.info(f"Ticket {ticket_number} does exist, passing")
            logger.info(f"check_for_existing_tickets: {check_for_existing_tickets}")
            continue
        else:
            logger.info(f"Ticket {ticket_number} does not exist, creating ticket")    
            logger.info(f'Data for ticket values is : {value}')
            
            for index, (timestamp, ticket_data) in enumerate(value):
                      
                if index == 0:
                    json_payload = syncro_prepare_ticket_combined_json(ticket_data)  
                    print(f"Creating new ticket: {ticket_number}")
                    response = syncro_create_ticket(json_payload)
                else:
                    print(f"Adding comment to ticket: {ticket_number}")
                    json_payload = syncro_prepare_ticket_combined_comment_json(ticket_data)  
                    response = syncro_create_comment(json_payload)

                    if response:
                        print(f"Successfully created comment: {ticket_number}")
                    else:
                        print(f"Failed Comment: {ticket_number}")

            logger.info(f"Completed Ticket {ticket_number}")  




            



if __name__ == "__main__":
    main()
