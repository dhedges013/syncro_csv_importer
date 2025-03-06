# syncro_csv_import
 unoffical ticket and comment import tool


# To-Do List 2.27.25
-Template CSV Data points are not unified

combined_ticket and comment JSON builders doesnt account for:
   - the tech assigned for ticket creation 
   - if the comment should be private communication or not.


# Syncro Ticket Importer

## Setup Instructions

1. **Configure Syncro API Access**  
   - Add your **Syncro Subdomain** and **API Key** in the `syncro_configs` file.
   - Adjust your Timezone if needed as well

2. **Import Process & Temporary Data**  
   - To speed up the import process, the importer generates a `syncro_temp_data.json` file on the first run of `main_tickets.py`.  
   - If you add new **Techs, Customers, Contacts, Ticket Issue Types, Statuses, etc.
   - As of 2.24.25 **, you **must delete this file** to allow the importer to rebuild it on the next run.

3. **Logs & File Management**  
   - Log files are stored in the `logs` folder.  
   - A new log file is created for each run.

## Preparing Your Ticket Data

1. **Review & Prepare CSV Files**  
   - Review `ticket_comment_template.csv` and `tickets_template.csv`.  
   - Clone these template files and **add your data**.  
   - Rename them to:
     - `ticket_comments.csv`
     - `tickets.csv`

2. **Running the Importer**  
   - Run `main_tickets.py` to load in the tickets.  
   - Run `main_comments.py` to load in the comments.

## Testing with Sample Data

- Test data is available in the **Test Data CSV** folder for trial runs.

## Important Notes

- When creating a new ticket, the **initial issue timestamp** will be set at the time of import.  
- The **ticket itself** will have the correct created date.



## Done List

3.5.25
Added CLI options to run the tool. Will prompt for subdomain and API Key
Added option for doing Intial Ticket Import or Comment Import but typing a 1 or a 2 from a menu select
Added Data Validation when loading in CSV Data so it cannot be blank