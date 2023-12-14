import logging

def print_colored_error(message):
    print(f"\033[91m{message}\033[0m")

def log_error(message):
    seperator = "x"
    line = ""
    for i in range(80):
        line += seperator

    print_colored_error(line)
    print_colored_error(f"ERROR: {message}")
    print_colored_error(line)

    logging.error(f"{message}")
   
# Function to log and print messages
def log_info(message):
    print(message)
    logging.info(message)

def log_separator(seperator="-"):
    line = ""
    for i in range(80):
        line += seperator
        
    log_info(line)

def log_execution_time(start_time, end_time, message="Execution time"):
    execution_time = end_time - start_time

    # Format the execution time as hh:mm:ss:mmmm
    hours, remainder = divmod(execution_time, 3600)
    minutes, remainder = divmod(remainder, 60)
    seconds, milliseconds = divmod(remainder, 1)

    formatted_time = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}:{int(milliseconds * 1000):04}"
    log_info(f"{message}: {formatted_time}")

