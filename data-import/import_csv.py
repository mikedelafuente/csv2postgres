import os
import psycopg2
import csv
import re
import time
import sys
import logging
import traceback

def log_and_print(message):
    print(message)
    logging.info(message)

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
            time.sleep(delay_seconds)
    return False

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
        int_pattern = re.compile(r'^-?\d+$')
        float_pattern = re.compile(r'^-?\d+\.\d+$')
        bool_pattern = re.compile(r'^(true|false)$', re.IGNORECASE)
        uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)

        # Default VARCHAR size for columns not explicitly defined
        default_varchar_size = 255

        # Initialize column_sizes to have enough elements for all columns
        column_sizes = [f''] * len(header)
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
                        column_data_types[i] = 'INTEGER'
                    # Check if the cell matches a floating-point number pattern
                    elif float_pattern.match(cell):
                        column_data_types[i] = 'NUMERIC'
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
                        # Default to VARCHAR if no other match
                        column_data_types[i] = 'VARCHAR'

                    # Track max VARCHAR length for VARCHAR columns
                    if column_data_types[i] == 'VARCHAR':
                        max_length = len(cell)
                        if max_length > int(column_sizes[i].strip('()') or '255'):
                            column_sizes[i] = f'({max_length})'
            except Exception as e:
                error_message = f"Error processing row ${row_count}: {e}\n{traceback.format_exc()}"
                logging.error(error_message)
                print(error_message)

        return header, column_data_types, column_sizes

# Function to read table definitions from schema files
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
                    log_and_print(f"Column Name: {column_name} || Data Type: {data_type}")
             
                    column_size = ''
                    # Check if a column size is specified
                    if '(' in data_type:
                        start_index = data_type.find('(')
                        end_index = data_type.find(')')
                        if start_index != -1 and end_index != -1:
                            column_size = data_type[start_index:end_index+1]
                            # Remove the size part from data_type
                            data_type = data_type[:start_index]

                            log_and_print(f"matched: col_size: {column_size} ||  data_type: {data_type}")
                   
                        else:
                            log_and_print("No match found")
                    else:
                        # Default to nothing if no column size specified
                        column_size = ''
                    
                    # Append column name and data type to their respective lists/dictionaries
                    header.append(column_name.strip())
                    data_types.append(data_type.strip())
                    column_sizes.append(column_size.strip())
                else:
                    log_and_print(f"Column Name: {column_name}")
            
                    # Default to VARCHAR(255) if no data type specified
                    header.append(column_name.strip())
                    data_types.append('VARCHAR')
                    column_sizes.append('(255)')
            
    return header, data_types, column_sizes


# Function to create a database table
def create_table(cursor, table_name, header, data_types, column_sizes):
    # Create the table with the inferred data types and sizes
    columns_sql = ', '.join([f'"{column_name.lower()}" {data_types[i]}{column_sizes[i]}'
                            for i, column_name in enumerate(header)])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {columns_sql}
    );
    """
    cursor.execute(create_table_sql)
    log_and_print(f"Schema created for table {table_name}: {create_table_sql}")

# Function to insert data into a database table
# Function to insert data into a database table
def insert_data(conn, cursor, table_name, header, csv_file, commit_every=50):
    with open(csv_file, 'r') as csv_file_handle:
        log_and_print(f"Opening CSV file: {csv_file}")
        csv_reader = csv.reader(csv_file_handle)
        next(csv_reader)  # Skip header row

        # Construct the INSERT statement with lowercase column names
        insert_sql = f'INSERT INTO "{table_name}" ({", ".join(header).lower()}) VALUES ({", ".join(["%s"] * len(header))})'

        # Insert data into the table, logging errors
        row_count = 0
        for row in csv_reader:
            row_count += 1
            try:
                cursor.execute(insert_sql, row)
                # log_and_print(f"Data inserted into table {table_name}: {row}")
            except Exception as e:
                logging.error(f"Error inserting row {row} into {table_name}: {e}")

            # Commit every 'commit_every' rows
            if row_count % commit_every == 0:
                conn.commit()

        # Commit any remaining rows
        conn.commit()

        log_and_print(f"Inserted {row_count} rows in {table_name}")

# Database connection parameters
db_params = {
    'dbname': 'mydb',
    'user': 'myuser',
    'password': 'mypassword',
    'host': 'postgres',
    'port': '5432',
}

# Set up logging
logging.basicConfig(filename='import_log.log', level=logging.INFO)

log_and_print("Starting Import CSV script")

# Wait for PostgreSQL server to start
if not wait_for_postgres(**db_params):
    log_and_print("Error: Unable to connect to PostgreSQL server.")
    sys.exit(1)

log_and_print("PostgreSQL server is ready. Proceeding with data insertion.")

# Iterate through all CSV files in the 'data' folder
for root, dirs, files in os.walk('data'):
    for file in files:
        if file.endswith('.csv'):
            csv_file = os.path.join(root, file)
            table_name = os.path.splitext(file)[0]  # Extract table name from file name (excluding extension)
            schema_file = os.path.join(root, f"{table_name}.schema")  # Path to corresponding schema file

            try:
                conn = psycopg2.connect(**db_params)
                cursor = conn.cursor()

                # Drop the table if it exists
                drop_table_sql = f'DROP TABLE IF EXISTS "{table_name}"'
                cursor.execute(drop_table_sql)

                if os.path.exists(schema_file):
                    header, data_types, column_sizes = read_table_definition(schema_file)
                else:
                    header, data_types, column_sizes = infer_data_types_and_sizes(csv_file)

                create_table(cursor, table_name, header, data_types, column_sizes)
                conn.commit()
                insert_data(conn, cursor, table_name, header, csv_file)

                conn.commit()
                log_and_print(f"Data inserted into table {table_name} successfully.")
            except Exception as e:
                error_message = f"Error processing {csv_file}: {e}\n{traceback.format_exc()}"
                logging.error(error_message)
                print(error_message)
            finally:
                cursor.close()
                conn.close()
