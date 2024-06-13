# Yahoo Finance to PostgreSQL Data Pipeline

This project contains an Apache Airflow DAG for extracting stock data from Yahoo Finance and storing it in a PostgreSQL database. The pipeline is designed to be modular, maintainable, and production-ready, following best practices for configuration management, logging, and error handling.

## Prerequisites

- Apache Airflow
- Python 3.10+
- PostgreSQL database
- Environment variables for database configuration
- Dbt-core

## Environment Variables

Before running the DAG, ensure you have the following environment variables set for database credentials:

- `DB_NAME`: PostgreSQL database name 
- `DB_USER`: PostgreSQL user 
- `DB_PASSWORD`: PostgreSQL password 
- `DB_HOST`: PostgreSQL host
- `DB_PORT`: PostgreSQL port

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/your-username/your-repo-name.git
    cd your-repo-name
    ```

2. Create a virtual environment and activate it:
    ```sh
    python3 -m venv airflow-venv
    source airflow-venv/bin/activate
    ```

3. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

4. Set up Airflow:
    ```sh
    export AIRFLOW_HOME=$(pwd)/airflow
    airflow db init
    ```

## Configuration

Ensure that the required environment variables are set. You can add them to your shell profile or export them directly in your terminal session:

```sh
export DB_NAME='your_db_name'
export DB_USER='your_db_user'
export DB_PASSWORD='your_db_password'
export DB_HOST='your_db_host'
export DB_PORT='your_db_port'
