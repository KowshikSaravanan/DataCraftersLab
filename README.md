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

### Machine Learning

1. **Fetching Data**: The DAG starts by fetching data from a PostgreSQL database table named `stock_data_gold`.

2. **Data Preprocessing**: The fetched data is preprocessed, including converting the 'date' column to a datetime object and setting it as the index.

3. **Feature Engineering**: For each unique symbol in the data, a linear regression model is trained using the 'day' as a feature and the 'close' price as the target variable.

4. **Prediction**: Future close prices are predicted for the next 30 days using the trained linear regression model.

5. **Saving Predictions**: The predicted future prices along with their corresponding dates and symbols are saved to a PostgreSQL table named `future_predictions`.

6. **Error Handling**: If an error occurs during any step, the task is retried once.

7. **Airflow Integration**: The DAG is designed to be run in Apache Airflow, a platform for programmatically authoring, scheduling, and monitoring workflows.

This summary highlights the main steps and objectives of the prediction DAG, focusing on data processing, model training, prediction, and result storage.