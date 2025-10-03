# Open Issues
When pulling in the Cache'd Customer list if the Business Name is "BLANK" or Null there is no error handing for this
When Attempting to create a ticket if there is no Customer ID found on the lookup, the import still tries to create a ticket resulting in an error. needs Validation checks before hand and error handing to stop the import 

# syncro_csv_import
 unoffical ticket and comment import tool

# To Run the Import
- run the cli.py python file
- see requirements.txt for list of required python modules
- you should be prompted with questions when running cli.py like select your:
   - Level of Logging
   - Syncro Subdomain and API Key
   - Clearing out the temp_data.json file (if there is one already created)

# To-Do List 3.16.25
-Template CSV Data points are not unified

Importer doesnt account for:
   - if the comment should be private or public communication or not. All comments are imported in as Private


# Syncro Ticket Importer

## Setup Instructions

1. **Configure Syncro API Access**
   - Add your **Syncro Subdomain** and **API Key** in the `syncro_configs` file.
   - Adjust your Timezone if needed as well
   - Choose your preferred timestamp format (`US` for MM/DD/YY or `INTL` for DD/MM/YY)
   - The importer recognizes many date/time styles (e.g. `1-2-2025`, `15-2-04`, `05/04/25 5:04 PM`)

2. **Import Process & Temporary Data**  
   - To speed up the import process, the importer generates a `syncro_temp_data.json` file on the first run of `cli.py`.  
   - If you add new **Techs, Customers, Contacts, Ticket Issue Types, Statuses, etc. you should delete this file and let the program recreate it
   - You are prompted to update (delete the file to be recreated) or keep this temp data on each run

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
   - Option 4 will generate timer entries for the most recent 25 tickets using your techs and labor products.
   - Data is validated on csv load and will error out of the import without the csv being filled out


## Testing with Sample Data

- Test data is available in the **Test Data CSV** folder for trial runs.

## Important Notes

- When creating a new ticket, the **initial issue timestamp** will be set at the time of import.  
- The **ticket itself** will have the correct created date.

## Known Issue List

3.16.25 update
- testing with 500 contacts under one customer looked up worked

3.6.25
- pagination and contact cacheing did not work for a customer with 600 contacts


## Done List

3.16.25
- Reworked the logging and made it simpler instead of having seperate logging files; Logs will now print to same file
- Cleaned up passing around the "logger" inbetween functions, which is not required.
- Cleaned up and unified how they config object is passed inbetween functions. it shou\ld always be the first variable passed in.
- Fixed issue with Intial Issue, not importing on ticket creation with tickets.csv
- Updated template for comments - changed "ticket subject" to "comment subject"
- added _pause variable for rest inbetween API calls in the syncro_read.py file
- updated data validation to better handle lowercase and upper case mismatches
- NOTE: Ticket Status cannot be normalized and Case mismatch will throw a validation error

3.6.25
- Updated Contact Lookup to work correctly * but still having issues with pagination
- Flexiblity added with debug/info/warrning. Most things set to debug level
- started working on seperate logging for major errors with a new syncro_logging_configs

3.5.25
- Added CLI options to run the tool. Will prompt for subdomain and API Key
- Added option for doing Intial Ticket Import or Comment Import but typing a 1 or a 2 from a menu select
- Added Data Validation when loading in CSV Data so it cannot be blank


