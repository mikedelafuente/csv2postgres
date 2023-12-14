import psycopg2
import csv
import time
import traceback
from logger import log_error, log_info, log_execution_time

# Database connection parameters - this should match what docker-compose.yaml is set up to use
DB_PARAMS = {
    'dbname': 'mydb',
    'user': 'myuser',
    'password': 'mypassword',
    'host': 'postgres',
    'port': '5432',
}

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
        conn = psycopg2.connect(**DB_PARAMS)
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
        conn = psycopg2.connect(**DB_PARAMS)
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
                        conn = psycopg2.connect(**DB_PARAMS)
                    
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
    log_execution_time(start_time, end_time, f"Insertion time for table {table_name}")
    log_info(f"Inserted {row_count} rows in {table_name} using {commit_count} commits")

