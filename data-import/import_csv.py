import os
import psycopg2
import csv
import re
import time
import sys
import logging

print("Starting Import CSV script")

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

# Function to wait for PostgreSQL server to become available
def wait_for_postgres(host, port, username, password, dbname, max_attempts=30, delay_seconds=2):
    print("Running Wait For Postgres")
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=username,
                password=password,
                host=host,
                port=port
            )
            conn.close()
            print(f"Connected to PostgreSQL (attempt {attempt}/{max_attempts}).")
            return True
        except psycopg2.OperationalError:
            print(f"Waiting for PostgreSQL (attempt {attempt}/{max_attempts})...")
            time.sleep(delay_seconds)
    print("Error: Unable to connect to PostgreSQL server.")
    return False

# Wait for PostgreSQL server to start
if not wait_for_postgres(db_params['host'], db_params['port'], db_params['user'], db_params['password'], db_params['dbname']):
    print("Error: Unable to connect to PostgreSQL server.")
    sys.exit(1)

else:
    print("PostgreSQL server is ready. Proceeding with data insertion.")

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
        int_pattern = re.compile(r'^-?\d+$')
        float_pattern = re.compile(r'^-?\d+\.\d+$')
        bool_pattern = re.compile(r'^(true|false)$', re.IGNORECASE)

        # Analyze data to infer data types and estimate column sizes
        for row in reader:
            for i, cell in enumerate(row):
                # Check if the cell matches a date pattern
                if date_pattern.match(cell):
                    column_data_types[i] = 'DATE'
                # Check if the cell matches an integer pattern
                elif int_pattern.match(cell):
                    column_data_types[i] = 'INTEGER'
                # Check if the cell matches a floating-point number pattern
                elif float_pattern.match(cell):
                    column_data_types[i] = 'NUMERIC'
                # Check if the cell matches a boolean pattern
                elif bool_pattern.match(cell):
                    column_data_types[i] = 'BOOLEAN'

                # Estimate column size based on the length of the cell
                if i not in column_sizes or len(cell) > column_sizes[i]:
                    column_sizes[i] = len(cell)

        return header, column_data_types, column_sizes

# Iterate through all CSV files in the 'data' folder
for root, dirs, files in os.walk('data'):
    for file in files:
        if file.endswith('.csv'):
            csv_file = os.path.join(root, file)
            # Add a print statement to indicate the file being processed
            print(f"Processing CSV file: {csv_file}")

            table_name = os.path.splitext(file)[0]  # Extract table name from file name (excluding extension)

            try:
                conn = psycopg2.connect(**db_params)
                cursor = conn.cursor()

                header, data_types, column_sizes = infer_data_types_and_sizes(csv_file)


                # Generate column definitions for table creation
                column_definitions = []
                for i, column_name in enumerate(header):
                    data_type = data_types.get(i, 'VARCHAR')  # Default to VARCHAR
                    size = column_sizes.get(i, 255)  # Default size is 255

                    # Adjust the size to the nearest increment (e.g., 100 or 255)
                    increment = 100  # You can adjust this as needed
                    size = ((size - 1) // increment + 1) * increment

                    if data_type == 'VARCHAR':
                        column_definitions.append(f'"{column_name.lower()}" {data_type}({size})')
                    else:
                        column_definitions.append(f'"{column_name.lower()}" {data_type}')

                # Create the table with dynamically generated columns (all in lowercase)
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    {', '.join(column_definitions)}
                );
                """
                print(f"Creating table {table_name}...")
                cursor.execute(create_table_sql)
                conn.commit()
                print(f"Table {table_name} created successfully.")

                # Open the CSV file for reading
                with open(csv_file, 'r') as csv_file_handle:
                    csv_reader = csv.reader(csv_file_handle)
                    next(csv_reader)  # Skip header row

                    # Construct the INSERT statement with lowercase column names
                    insert_sql = f'INSERT INTO "{table_name}" ({", ".join(header).lower()}) VALUES ({", ".join(["%s"] * len(header))})'

                    # Insert data into the table, logging errors
                    for row in csv_reader:
                        try:
                            cursor.execute(insert_sql, row)
                            print(f"Inserted row into {table_name}: {row}")
                        except Exception as e:
                            logging.error(f"Error inserting row {row} into {table_name}: {e}")
                            print(f"Error inserting row {row} into {table_name}: {e}")

                conn.commit()
                print(f"Data inserted into table {table_name} successfully.")
            except Exception as e:
                logging.error(f"Error processing {csv_file}: {e}")
                print(f"Error processing {csv_file}: {e}")
            finally:
                cursor.close()
                conn.close()
