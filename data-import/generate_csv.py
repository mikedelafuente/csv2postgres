import csv
import os
from faker import Faker

fake = Faker()

# Define the number of rows you want in your CSV
num_rows = 100

# Define the CSV folder and file name
data_folder = "data"
csv_file = os.path.join(data_folder, "fake_data.csv")

# Create the "data" subfolder if it doesn't exist
os.makedirs(data_folder, exist_ok=True)

# Define the CSV header
header = ["Name", "Email", "Phone"]

# Generate fake data and write it to the CSV file
with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(header)

    for _ in range(num_rows):
        name = fake.name()
        email = fake.email()
        phone = fake.phone_number()

        row = [name, email, phone]
        writer.writerow(row)

print(f"Fake CSV data has been generated and saved to {csv_file}.")
