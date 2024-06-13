import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import yfinance as yf
import psycopg2
from psycopg2 import sql
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables for sensitive information
DB_NAME = os.getenv('DB_NAME', 'dbt')
DB_USER = os.getenv('DB_USER', 'dbt')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'dbt')
DB_HOST = os.getenv('DB_HOST', '<ip_address>')
DB_PORT = os.getenv('DB_PORT', '5432')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'yahoo_finance_to_postgres',
    default_args=default_args,
    description='Extract data from Yahoo Finance and store it in PostgreSQL for multiple symbols',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

symbols = ['AAPL', 'GOOGL', 'MSFT']  # List of stock symbols to extract data for

def create_table_if_not_exists(conn, table_name):
    try:
        with conn.cursor() as cursor:
            create_table_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    Date DATE,
                    Symbol VARCHAR(10),
                    Open NUMERIC,
                    High NUMERIC,
                    Low NUMERIC,
                    Close NUMERIC,
                    Volume BIGINT,
                    Dividends NUMERIC,
                    Stock_Splits NUMERIC,
                    PRIMARY KEY (Date, Symbol)
                )
            """).format(sql.Identifier(table_name))
            cursor.execute(create_table_query)
            conn.commit()
            logger.info("Table %s ensured to exist.", table_name)
    except Exception as e:
        logger.error("Error creating table %s: %s", table_name, e)
        raise

def get_max_date_from_postgres(symbol, **kwargs):
    table_name = kwargs['table_name']
    conn_params = kwargs['conn_params']
    try:
        conn = psycopg2.connect(**conn_params)
        create_table_if_not_exists(conn, table_name)
        
        with conn.cursor() as cursor:
            query = sql.SQL("SELECT MAX(Date) FROM {} WHERE Symbol = %s").format(sql.Identifier(table_name))
            cursor.execute(query, (symbol,))
            max_date = cursor.fetchone()[0]
        
        conn.close()
        kwargs['ti'].xcom_push(key=f'max_date_{symbol}', value=max_date)
        logger.info("Max date for symbol %s is %s", symbol, max_date)
    except Exception as e:
        logger.error("Error getting max date for symbol %s: %s", symbol, e)
        raise

def get_stock_data(symbol, **kwargs):
    try:
        max_date = kwargs['ti'].xcom_pull(key=f'max_date_{symbol}')
        start_date = max_date + timedelta(days=1) if max_date else '2020-01-01'
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        stock = yf.Ticker(symbol)
        data = stock.history(start=start_date, end=end_date)
        data['Symbol'] = symbol
        data.reset_index(inplace=True)
        data['Date'] = data['Date'].astype(str)
        
        kwargs['ti'].xcom_push(key=f'stock_data_{symbol}', value=data.to_dict())
        logger.info("Stock data for symbol %s extracted successfully.", symbol)
    except Exception as e:
        logger.error("Error getting stock data for symbol %s: %s", symbol, e)
        raise

def insert_data_into_postgres(symbol, **kwargs):
    table_name = kwargs['table_name']
    conn_params = kwargs['conn_params']
    try:
        data_dict = kwargs['ti'].xcom_pull(key=f'stock_data_{symbol}')
        data = pd.DataFrame.from_dict(data_dict)
        
        conn = psycopg2.connect(**conn_params)
        with conn.cursor() as cursor:
            for _, row in data.iterrows():
                cursor.execute(sql.SQL("""
                    INSERT INTO {} (Date, Symbol, Open, High, Low, Close, Volume, Dividends, Stock_Splits)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Date, Symbol) DO NOTHING
                """).format(sql.Identifier(table_name)),
                (row['Date'], row['Symbol'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume'], row['Dividends'], row['Stock Splits']))
        
        conn.commit()
        conn.close()
        logger.info("Stock data for symbol %s inserted successfully.", symbol)
    except Exception as e:
        logger.error("Error inserting data for symbol %s: %s", symbol, e)
        raise

for symbol in symbols:
    get_max_date_task = PythonOperator(
        task_id=f'get_max_date_{symbol}',
        python_callable=get_max_date_from_postgres,
        op_kwargs={
            'symbol': symbol,
            'table_name': 'stock_data',
            'conn_params': {
                'dbname': DB_NAME,
                'user': DB_USER,
                'password': DB_PASSWORD,
                'host': DB_HOST,
                'port': DB_PORT
            },
        },
        provide_context=True,
        dag=dag,
    )

    extract_task = PythonOperator(
        task_id=f'extract_stock_data_{symbol}',
        python_callable=get_stock_data,
        op_kwargs={
            'symbol': symbol,
        },
        provide_context=True,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id=f'load_data_to_postgres_{symbol}',
        python_callable=insert_data_into_postgres,
        op_kwargs={
            'symbol': symbol,
            'table_name': 'stock_data',
            'conn_params': {
                'dbname': DB_NAME,
                'user': DB_USER,
                'password': DB_PASSWORD,
                'host': DB_HOST,
                'port': DB_PORT
            },
        },
        provide_context=True,
        dag=dag,
    )

    get_max_date_task >> extract_task >> load_task
