# syncro_csv_import
 unoffical ticket and comment import tool


# To-Do List 3.16.25
-Template CSV Data points are not unified

Importer doesnt account for:
   - if the comment should be private or public communication or not. All comments are imported in as Private


# Syncro Ticket Importer

## Setup Instructions

1. **Configure Syncro API Access**  
   - Add your **Syncro Subdomain** and **API Key** in the `syncro_configs` file.
   - Adjust your Timezone if needed as well

2. **Import Process & Temporary Data**  
   - To speed up the import process, the importer generates a `syncro_temp_data.json` file on the first run of `cli.py`.  
   - If you add new **Techs, Customers, Contacts, Ticket Issue Types, Statuses, etc.
   - You are prompted to clear or keep this temp data on each run

3. **Logs & File Management**  
   - Log files are stored in the `logs` folder.  
   - A new log file is created for day.

## Preparing Your Ticket Data

1. **Review & Prepare CSV Files**  
   - Review `ticket_comment_template.csv` and `tickets_template.csv`.  
   - Clone these template files and **add your data**.  
   - Rename them to:
     - `ticket_comments.csv`
     - `tickets.csv`
   - Place in the root folder 

2. **Running the Importer**  
   - Run `cli.py` to load in the tickets or comments.  
   - You will be prompted to choose logging level, Clear Temp_Data, Enter Subdomain and API Key
   - Last will ask you to pick either Ticket or Comments to import.
   - Data is validated on csv load and will error out of the import without the csv being filled out


## Testing with Sample Data

- Test data is available in the **Test Data CSV** folder for trial runs.

## Important Notes

- When creating a new ticket, the **initial issue timestamp** will be set at the time of import.  
- The **ticket itself** will have the correct created date.

## Known Issue List

3.16.25 update
testing with 500 contacts under one customer looked up worked

3.6.25
pagination and contact cacheing did not work for a customer with 600 contacts


## Done List

3.16.25
Reworked the logging and made it simpler instead of having seperate logging files; Logs will now print to same file
Cleaned up passing around the "logger" inbetween functions, which is not required.
Cleaned up and unified how they config object is passed inbetween functions. it shou\ld always be the first variable passed in.
Fixed issue with Intial Issue, not importing on ticket creation with tickets.csv
Updated template for comments - changed "ticket subject" to "comment subject"
added _pause variable for rest inbetween API calls in the syncro_read.py file
updated data validation to better handle lowercase and upper case mismatches
NOTE: Ticket Status cannot be normalized and Case mismatch will throw a validation error

3.6.25
Updated Contact Lookup to work correctly * but still having issues with pagination
Flexiblity added with debug/info/warrning. Most things set to debug level
started working on seperate logging for major errors with a new syncro_logging_configs

3.5.25
Added CLI options to run the tool. Will prompt for subdomain and API Key
Added option for doing Intial Ticket Import or Comment Import but typing a 1 or a 2 from a menu select
Added Data Validation when loading in CSV Data so it cannot be blank


