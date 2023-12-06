# CSV to PostgreSQL Importer

This project is a CSV to PostgreSQL data importer that allows you to easily import CSV files into a PostgreSQL database using Docker containers.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- [Docker](https://docs.docker.com/get-docker/) installed on your local machine.

## Getting Started

To get this project up and running on your local machine, follow these steps:

1. Clone the repository:

   ```bash
   git clone <repository-url>
   ```

2. Navigate to the project directory:

   ```bash
   cd csv-to-postgresql-importer
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

2. Run the Docker containers as described in the "Getting Started" section.

3. Wait for the CSV data to be imported into the PostgreSQL database.

4. Access your PostgreSQL database to query and analyze the imported data.

## Cleanup

To stop and remove the Docker containers and clean up resources, run the following command:

```bash
./setup_environment.sh down
```

## Contributing

If you would like to contribute to this project, please follow our [Contribution Guidelines](CONTRIBUTING.md).

## License

This project is licensed under the [MIT] License - see the [LICENSE.md](LICENSE.md) file for details.
