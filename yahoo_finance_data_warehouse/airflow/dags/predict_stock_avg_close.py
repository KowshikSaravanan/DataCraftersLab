from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import datetime as dt
import psycopg2
from psycopg2 import sql
from sklearn.linear_model import LinearRegression
import numpy as np

# Define PostgreSQL connection credentials
POSTGRES_DB = 'dbt'
POSTGRES_USER = 'dbt'
POSTGRES_PASSWORD = 'dbt'
POSTGRES_HOST = '172.31.209.62'
POSTGRES_PORT = '5432'

def get_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )

def fetch_gold_layer_data():
    conn = get_connection()
    query = "SELECT * FROM stock_data_gold"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def predict_future_data(**kwargs):
    import pandas as pd
    from sklearn.linear_model import LinearRegression
    import numpy as np

    # Load the data
    df = kwargs['ti'].xcom_pull(task_ids='fetch_gold_layer_data')

    # Preprocess the data (example)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # Feature engineering and prediction (example)
    for symbol in df['symbol'].unique():
        symbol_df = df[df['symbol'] == symbol]
        symbol_df = symbol_df[['avg_close']].dropna()

        # Create features and target
        symbol_df['day'] = np.arange(len(symbol_df))
        X = symbol_df[['day']]
        y = symbol_df['avg_close']

        # Train the model
        model = LinearRegression()
        model.fit(X, y)

        # Predict future values
        future_days = 30
        future_X = np.arange(len(symbol_df), len(symbol_df) + future_days).reshape(-1, 1)
        future_y = model.predict(future_X)

        # Save predictions
        future_dates = pd.date_range(start=symbol_df.index[-1], periods=future_days, freq='D')
        assert len(future_dates) == len(future_y), "future_dates and future_y must be of the same length"
        future_df = pd.DataFrame({'date': future_dates, 'symbol': symbol, 'predicted_close': future_y})
        kwargs['ti'].xcom_push(key=f'{symbol}_future_predictions', value=future_df)




def save_predictions_to_postgres(**kwargs):
    conn = get_connection()
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS future_predictions_avg_close (
        date DATE NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        predicted_close FLOAT NOT NULL
    )
    """
    cursor.execute(create_table_query)

    truncate_table_query = """ truncate table future_predictions_avg_close """
    cursor.execute(truncate_table_query)

    for symbol in pd.read_sql("SELECT DISTINCT symbol FROM stock_data_gold", conn)['symbol']:
        future_df = kwargs['ti'].xcom_pull(key=f'{symbol}_future_predictions', task_ids='predict_future_data')

        for _, row in future_df.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO future_predictions_avg_close (date, symbol, predicted_close)
                VALUES (%s, %s, %s)
            """)
            cursor.execute(insert_query, (row['date'], row['symbol'], row['predicted_close']))

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    'predict_stock_avg_close',
    default_args=default_args,
    description='Predict future stock data from the gold layer',
    schedule_interval=None,
    start_date=dt.datetime(2023, 6, 16),
    catchup=False,
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_gold_layer_data',
        python_callable=fetch_gold_layer_data,
    )

    predict_data = PythonOperator(
        task_id='predict_future_data',
        python_callable=predict_future_data,
        provide_context=True,
    )

    save_data = PythonOperator(
        task_id='save_predictions_to_postgres',
        python_callable=save_predictions_to_postgres,
        provide_context=True,
    )

    fetch_data >> predict_data >> save_data
