import os
import psycopg2
import csv
import re
import time
import sys
import logging
import traceback

# Database connection parameters - this should match what docker-compose.yaml is set up to use
db_params = {
    'dbname': 'mydb',
    'user': 'myuser',
    'password': 'mypassword',
    'host': 'postgres',
    'port': '5432',
}

def print_error(message):
    print(f"\033[91m{message}\033[0m")

def log_error(message):
    seperator = "x"
    line = ""
    for i in range(80):
        line += seperator

    print_error(line)
    print_error(f"ERROR: {message}")
    print_error(line)

    logging.error(f"{message}")
   
# Function to log and print messages
def log_info(message):
    print(message)
    logging.info(message)

def log_and_print_separator(seperator="-"):
    line = ""
    for i in range(80):
        line += seperator
        
    log_info(line)

def log_and_print_execution_time(start_time, end_time, message="Execution time"):
    execution_time = end_time - start_time

    # Format the execution time as hh:mm:ss:mmmm
    hours, remainder = divmod(execution_time, 3600)
    minutes, remainder = divmod(remainder, 60)
    seconds, milliseconds = divmod(remainder, 1)

    formatted_time = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}:{int(milliseconds * 1000):04}"
    log_info(f"{message}: {formatted_time}")

# Function to wait for PostgreSQL server to become available
def wait_for_postgres(host, port, user, password, dbname, max_attempts=30, delay_seconds=2):
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            conn.close()
            return True
        except psycopg2.OperationalError:
            log_info(f"Attempt {attempt} of {max_attempts}: PostgreSQL server not available. Waiting...")
            time.sleep(delay_seconds)        
    return False

# Function to create a database table
def create_table(table_name, header, data_types, column_sizes):
    # Create the table with the inferred data types and sizes
    columns_sql = ', '.join([f'"{column_name.lower()}" {data_types[i]}{column_sizes[i]}'
                            for i, column_name in enumerate(header)])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns_sql}
    );
    """
    
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        cursor.execute(create_table_sql)
        conn.commit()
            
    except Exception as e:
        log_error(f"Error creating table {table_name}: {e}")
        cursor.rollback()
    finally:
        cursor.close()
        conn.close()

    log_info(f"Schema created for table {table_name}: {create_table_sql}")

# Function to drop a database table by name
def drop_table(table_name):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        # Drop the table if it exists
        drop_table_sql = f'DROP TABLE IF EXISTS "{table_name}"'
        cursor.execute(drop_table_sql)

    except Exception as e:
        log_error(f"Error dropping table {table_name}: {e}\n{traceback.format_exc()}")
    finally:
        cursor.close()
        conn.close()

# Function to insert data into a database table
def insert_data(table_name, header, csv_file, commit_every=50):
    row_count = 0
    conn = None
    cursor = None
    commit_count = 0
    has_inserts = False

    try:
        start_time = time.time()
        with open(csv_file, 'r') as csv_file_handle:
            log_info(f"Opening CSV file: {csv_file}")
            csv_reader = csv.reader(csv_file_handle)
            next(csv_reader)  # Skip header row

            for row in csv_reader:
                row_count += 1
                try:
                    if conn is None:
                        conn = psycopg2.connect(**db_params)
                    
                    if cursor is None:
                        cursor = conn.cursor()

                    # Construct the INSERT statement with lowercase column names
                    insert_sql = f'INSERT INTO "{table_name}" ({", ".join(header).lower()}) VALUES ({", ".join(["%s"] * len(header))})'
                    has_inserts = True
                    cursor.execute(insert_sql, row)

                    if row_count % commit_every == 0:
                        if cursor:
                            cursor.close()
                            cursor = None
                        if conn:
                            conn.commit()
                            conn.close()
                            conn = None
                            commit_count += 1
                            has_inserts = False

                except Exception as e:
                    log_error(f"Error inserting row {row_count} into {table_name}: {e}")
                    if conn:
                        conn.rollback()

    except Exception as e:
        log_error(f"Error creating a connection: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.commit()
            conn.close()
            if has_inserts == True:
                commit_count += 1

    end_time = time.time()
    log_and_print_execution_time(start_time, end_time, f"Insertion time for table {table_name}")
    log_info(f"Inserted {row_count} rows in {table_name} using {commit_count} commits")

# Function to infer data types and estimate column sizes
def infer_data_types_and_sizes(csv_file):
    with open(csv_file, 'r') as file_to_read:
        reader = csv.reader(file_to_read)
        header = next(reader)  # Get the header row

        # Initialize data type and column size dictionaries
        column_data_types = {}
        column_sizes = {}

        # Regular expressions for pattern matching
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        time_pattern = re.compile(r'^\d{2}:\d{2}:\d{2}$')
        timestamp_pattern = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
        int_pattern = re.compile(r'^0$|^-?[1-9]\d*$')
        float_pattern = re.compile(r'^0\.\d+$|^-?[1-9]\d*\.\d+$')
        bool_pattern = re.compile(r'^(true|false)$', re.IGNORECASE)
        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)

        # Default VARCHAR size for columns not explicitly defined
        default_varchar_size = 255

        # Initialize column_sizes to have enough elements for all columns
        column_sizes = [''] * len(header)
        row_count = 0

        # Analyze data to infer data types and estimate column sizes
        for row in reader:
            row_count += 1
        
            try:
                for i, cell in enumerate(row):
                    # Check if the cell matches a date pattern
                    if date_pattern.match(cell):
                        column_data_types[i] = 'DATE'
                    # Check if the cell matches a time pattern
                    elif time_pattern.match(cell):
                        column_data_types[i] = 'TIME'
                    # Check if the cell matches a timestamp pattern
                    elif timestamp_pattern.match(cell):
                        column_data_types[i] = 'TIMESTAMP'
                    # Check if the cell matches an integer pattern
                    elif int_pattern.match(cell):
                        if i not in column_data_types:
                            column_data_types[i] = 'INTEGER'
                        elif column_data_types[i] != 'INTEGER':
                            column_data_types[i] = 'VARCHAR'
                    # Check if the cell matches a floating-point number pattern
                    elif float_pattern.match(cell):
                        if i not in column_data_types:
                            column_data_types[i] = 'NUMERIC'
                        elif column_data_types[i] != 'NUMERIC':
                            column_data_types[i] = 'VARCHAR'
                    # Check if the cell matches a boolean pattern
                    elif bool_pattern.match(cell):
                        column_data_types[i] = 'BOOLEAN'
                    # Check if the cell matches a UUID pattern
                    elif uuid_pattern.match(cell):
                        column_data_types[i] = 'UUID'
                    # Check if the cell matches a JSON or JSONB pattern
                    elif cell.startswith('{') and cell.endswith('}') or cell.startswith('[') and cell.endswith(']'):
                        column_data_types[i] = 'JSONB'
                    else:
                        column_data_types[i] = 'VARCHAR'

                    # Track max VARCHAR length for VARCHAR columns
                    if column_data_types[i] == 'VARCHAR':
                        max_length = len(cell)
                        if max_length > int(column_sizes[i].strip('()') or '255'):
                            column_sizes[i] = f'({max_length})'
            except Exception as e:
                log_error("Error processing row {row_count}: {e}\n{traceback.format_exc()}")                

        return header, column_data_types, column_sizes

# Function to read table definitions from schema files
def read_table_definition(schema_file):
    header = []            # To store column names
    data_types = []        # To store data types
    column_sizes = []

    with open(schema_file, 'r') as schema_file_handle:
        for line in schema_file_handle:
            # Split each line using space
            parts = line.strip().split(' ')
            # Remove empty parts and strip whitespace
            parts = [part.strip() for part in parts if part.strip()]
            if parts:
                # Extract column name
                column_name = parts[0]
                if len(parts) > 1:
                    # Extract data type
                    data_type = ' '.join(parts[1:])  # Combine all parts from parts[1] and beyond
                    log_info(f"Column Name: {column_name} || Data Type: {data_type}")
             
                    column_size = ''
                    # Check if a column size is specified
                    if '(' in data_type:
                        start_index = data_type.find('(')
                        end_index = data_type.find(')')
                        if start_index != -1 and end_index != -1:
                            column_size = data_type[start_index:end_index+1]
                            # Remove the size part from data_type
                            data_type = data_type[:start_index]

                            log_info(f"Matched: col_size: {column_size} ||  data_type: {data_type}")
                   
                        else:
                            log_info("No match found")
                    else:
                        # Default to nothing if no column size specified
                        column_size = ''
                    
                    # Append column name and data type to their respective lists/dictionaries
                    header.append(column_name.strip())
                    data_types.append(data_type.strip())
                    column_sizes.append(column_size.strip())
                else:
                    log_info(f"Column Name: {column_name}")
            
                    # Default to VARCHAR(255) if no data type specified
                    header.append(column_name.strip())
                    data_types.append('VARCHAR')
                    column_sizes.append('(255)')
            
    return header, data_types, column_sizes

# Function to create a schema file
def create_schema_file(csv_file, schema_file):
    # Infer data types and sizes
    header, data_types, column_sizes = infer_data_types_and_sizes(csv_file)

    # Write the header and data types to a schema file
    with open(schema_file, 'w') as schema_file_handle:
        for i, column_name in enumerate(header):
            schema_file_handle.write(f'{column_name} {data_types[i]}{column_sizes[i]}\n')

    log_info(f"Schema file created: {schema_file}")
    
    # Log the contents of the schema file
    with open(schema_file, 'r') as schema_file_handle:
        log_info(f"Schema file contents: {schema_file_handle.read()}")

# Parses subfolders of this directory looking for CSV files, it is not a recursive search
def parse_folders(folders):
    for folder in folders:
        log_and_print_separator("+")
        log_info(f"Processing folder: {folder}")

        # Do not error out if the folder does not exist
        if not os.path.exists(folder):
            log_info(f"Folder {folder} does not exist.")
            # Skip to the next folder
            continue
        
        parse_folder_files(folder)

# Parses the files within a given folder        
def parse_folder_files(folder):
    for root, dirs, files in os.walk(folder):
        for file in files:
            log_and_print_separator()
            # check lowercase file extension
            # if the file is not a CSV file, skip it
            if not file.lower().endswith('.csv'):
                log_info(f"Skipping file {file} because it is not a CSV file.")
                continue

            log_info(f"Processing file: {file}")
            csv_file = os.path.join(root, file)
            table_name = os.path.splitext(file)[0]  # Extract table name from file name (excluding extension)
            schema_file = os.path.join(root, f"{table_name}.schema")  # Path to corresponding schema file

            try:
                drop_table(table_name)  # Drop the table if it exists
                log_info(f"Checking for schema file: {schema_file}")
                if not os.path.exists(schema_file):
                    create_schema_file(csv_file, schema_file)
                    
                if os.path.exists(schema_file):
                    header, data_types, column_sizes = read_table_definition(schema_file)  

                create_table(table_name, header, data_types, column_sizes)
                insert_data(table_name, header, csv_file)
                
                log_info(f"Data inserted into table {table_name} successfully.")
            except Exception as e:
                log_error(f"Error processing {csv_file}: {e}\n{traceback.format_exc()}")                

# Main entry point      
def main():
    # Set up logging
    logging.basicConfig(filename='import_log.log', level=logging.INFO)

    log_info("Starting Import CSV script")

    # Start the timer
    start_time = time.time()

    # Wait for PostgreSQL server to start
    if not wait_for_postgres(**db_params):
        log_info("Error: Unable to connect to PostgreSQL server.")
        sys.exit(1)

    log_info("PostgreSQL server is ready. Proceeding with data insertion.")

    # Iterate through all CSV files in the 'data' folder
    # Walk through both the sample data folder and the data folder
    folders = ['data', 'sample_data']

    # Get the full path of each folder to be parsed
    folders = [os.path.join(os.getcwd(), folder) for folder in folders]

    parse_folders(folders)

    log_and_print_separator("*")
    end_time = time.time()
    log_and_print_execution_time(start_time, end_time, "Main execution time")
    
    log_info("Import CSV script completed. Ready for queries.")

# Run the main function
if __name__ == "__main__":
    main()
