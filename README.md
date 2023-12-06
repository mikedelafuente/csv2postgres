# CSV to PostgreSQL Importer

This project is a CSV to PostgreSQL data importer that allows you to easily import CSV files into a PostgreSQL database using Docker containers.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- [Docker](https://docs.docker.com/get-docker/) installed on your local machine.

## Getting Started

To get this project up and running on your local machine, follow these steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/mikedelafuente/csv2postgres.git
   ```

2. Navigate to the project directory:

   ```bash
   cd csv2postgres
   ```

3. Build and run the Docker containers:

   ```bash
   ./setup_environment.sh
   ```

   This command will build the Docker images and start the PostgreSQL database and the CSV importer container.

4. Wait for the containers to start and execute the CSV import script.

   The PostgreSQL container will initialize the database schema using the `init.sql` script, and the CSV importer container will run the `import_csv.py` script to import CSV data into the database.

5. Once the containers are running and the data is imported, you can access your PostgreSQL database as needed.

## Usage

To use the CSV to PostgreSQL importer, follow these steps:

1. Place your CSV files in the `data` directory.

2. Create a `.schema` file (e.g., `your_table_name.schema`) for each CSV file if you want to specify the database schema manually. The `.schema` file should contain the table schema in the following format:

   ```
   column1_name data_type,
   column2_name data_type,
   ...
   ```

   For example, if your CSV has columns "name" (text) and "age" (integer), the corresponding `.schema` file would look like:

   ```
   name text,
   age integer
   ```

   If you don't create a `.schema` file, the importer will infer the schema from the CSV file.

3. Run the Docker containers as described in the "Getting Started" section.

4. Wait for the CSV data to be imported into the PostgreSQL database.

5. Access your PostgreSQL database to query and analyze the imported data.

## PostgreSQL Data Types

Here is a list of common PostgreSQL data types that you can use in your `.schema` file:

- `smallint`: 2-byte signed integer
- `integer`: 4-byte signed integer
- `bigint`: 8-byte signed integer
- `real`: 4-byte floating-point number
- `double precision`: 8-byte floating-point number
- `numeric`: Arbitrary precision number
- `text`: Variable-length character string
- `char(n)`: Fixed-length character string of length `n`
- `varchar(n)`: Variable-length character string with a maximum length of `n`
- `date`: Date (year, month, day)
- `time`: Time of day (hour, minute, second)
- `timestamp`: Date and time (year, month, day, hour, minute, second)
- `boolean`: `true` or `false` logical value
- `uuid`: Universally unique identifier
- `json`: Binary JSON data
- `jsonb`: Binary JSON data with decomposed storage

You can specify these data types in your `.schema` file when defining the table schema.

## Cleanup

To stop and remove the Docker containers and clean up resources, run the following command:

```bash
./setup_environment.sh down
```

## Contributing

If you would like to contribute to this project, please follow our [Contribution Guidelines](CONTRIBUTING.md).

## License

This project is licensed under the [MIT] License - see the [LICENSE.md](LICENSE.md) file for details.
