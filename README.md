# Yahoo Finance Data Warehouse

This project contains an Apache Airflow DAG for extracting stock data from Yahoo Finance and storing it in a PostgreSQL database. The pipeline is designed to be modular, maintainable, and production-ready, following best practices for configuration management, logging, and error handling.

## Prerequisites

- Apache Airflow
- Python 3.10+
- PostgreSQL database
- Environment variables for database configuration
- Dbt-core

## DBT Project Structure

The DBT project processes the stock data through three distinct layers: bronze, silver, and gold. Each layer serves a specific purpose in the data transformation pipeline.

### Bronze Layer

The bronze layer contains raw data ingested from Yahoo Finance. This layer stores the data as-is, without any transformations or aggregations. It serves as the single source of truth for the raw data and provides a foundation for further transformations.

- **Purpose**: Store raw data from Yahoo Finance.
- **Content**: Contains all columns directly from the source data (e.g., date, symbol, open, high, low, close, volume, dividends, stock splits).

### Silver Layer

The silver layer applies cleaning and enrichment transformations to the raw data from the bronze layer. This includes calculating daily changes, moving averages, and volatility. The goal is to prepare the data for higher-level analysis and reporting.

- **Purpose**: Clean and enrich the raw data.
- **Transformations**:
  - Calculate previous close, daily change, and daily change percentage.
  - Compute weekly and monthly moving averages.
  - Calculate monthly volatility.

### Gold Layer

The gold layer provides aggregated data for high-level insights and anomaly detection. This layer generates daily summaries and identifies anomalies based on daily change percentages. It is designed for reporting and analytical purposes.

- **Purpose**: Aggregate data for insights and anomaly detection.
- **Transformations**:
  - Aggregate daily data (e.g., average open, high, low, close, total volume, dividends, stock splits).
  - Detect anomalies based on significant daily changes.
  - Generate a final dataset for reporting and analysis.

By structuring the DBT project into these layers, we ensure a clear separation of concerns, making the data pipeline more maintainable, scalable, and easier to understand.

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
