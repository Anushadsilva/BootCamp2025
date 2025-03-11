import requests
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import datetime, timedelta
from include.eczachly.trino_queries import execute_trino_query, run_trino_query_dq_check
from airflow.models import Variable
import pandas as pd
import json

# Fetch credentials
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
polygon_credentials = json.loads(Variable.get('POLYGON_CREDENTIALS'))
polygon_api_key = polygon_credentials['AWS_SECRET_ACCESS_KEY']

schema = 'anushapamalidsilva24909'

@dag(
    description="A DAG for fetching Polygon stock data and aggregating it",
    default_args={
        "owner": schema,
        "start_date": datetime(2025, 1, 9),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
    },
    schedule_interval="@yearly",
    catchup=True,
    tags=["stock_data"],
)
def stock_data_pipeline():
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    production_table = f'{schema}.daily_stock_prices'
    staging_table = f'{production_table}_stg_{{{{ ds_nodash }}}}'

    def fetch_polygon_data(table, **kwargs):
        ds = kwargs['ds']
        for stock in maang_stocks:
            url = f"https://api.polygon.io/v2/aggs/ticker/{stock}/range/1/day/{ds}/{ds}?adjusted=true&sort=asc&apiKey={polygon_api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json().get('results', [])
                df = pd.DataFrame(data)
                if not df.empty:
                    execute_trino_query(f"INSERT INTO {table} VALUES {df.to_records()}")
            else:
                raise ValueError(f"Failed to fetch data for {stock}")

    create_staging_table = PythonOperator(
        task_id="create_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {staging_table} (
                    stock_symbol VARCHAR,
                    trade_date DATE,
                    open_price DOUBLE,
                    close_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    volume BIGINT
                )
            """
        }
    )

    load_to_staging = PythonOperator(
        task_id="load_to_staging",
        python_callable=fetch_polygon_data,
        op_kwargs={'table': staging_table}
    )

    run_dq_check = PythonOperator(
        task_id="run_dq_check",
        python_callable=run_trino_query_dq_check,
        op_kwargs={'query': f"SELECT stock_symbol, COUNT(*) FROM {staging_table} WHERE open_price IS NULL OR close_price IS NULL GROUP BY stock_symbol"}
    )

    create_production_table = PythonOperator(
        task_id="create_production_table",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE TABLE IF NOT EXISTS {production_table} (
                    stock_symbol VARCHAR,
                    trade_date DATE,
                    avg_open DOUBLE,
                    avg_close DOUBLE,
                    avg_high DOUBLE,
                    avg_low DOUBLE,
                    total_volume BIGINT
                )
            """
        }
    )

    insert_into_production = PythonOperator(
        task_id="insert_into_production",
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                INSERT INTO {production_table}
                SELECT 
                    COALESCE(s.stock_symbol, p.stock_symbol) AS stock_symbol,
                    COALESCE(s.trade_date, p.trade_date) AS trade_date,
                    AVG(COALESCE(s.open_price, p.avg_open)) AS avg_open,
                    AVG(COALESCE(s.close_price, p.avg_close)) AS avg_close,
                    AVG(COALESCE(s.high_price, p.avg_high)) AS avg_high,
                    AVG(COALESCE(s.low_price, p.avg_low)) AS avg_low,
                    SUM(COALESCE(s.volume, p.total_volume)) AS total_volume
                FROM {staging_table} s
                FULL OUTER JOIN {production_table} p
                ON s.stock_symbol = p.stock_symbol AND s.trade_date = p.trade_date
                WHERE COALESCE(s.trade_date, p.trade_date) = DATE('{{{{ ds }}}}')
                GROUP BY stock_symbol, trade_date;
            """
        }
    )

    drop_staging_table = PythonOperator(
        task_id="drop_staging_table",
        python_callable=execute_trino_query,
        op_kwargs={'query': f"DROP TABLE IF EXISTS {staging_table}"}
    )

    calculate_rolling_avg = PythonOperator(
        task_id="calculate_rolling_avg",
        depends_on_past=True,
        python_callable=execute_trino_query,
        op_kwargs={
            'query': f"""
                CREATE OR REPLACE TABLE {schema}.daily_stock_prices_7d AS
                SELECT stock_symbol, trade_date,
                       AVG(avg_open) OVER (PARTITION BY stock_symbol ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_open,
                       AVG(avg_close) OVER (PARTITION BY stock_symbol ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_close,
                       AVG(avg_high) OVER (PARTITION BY stock_symbol ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_high,
                       AVG(avg_low) OVER (PARTITION BY stock_symbol ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_low,
                       SUM(total_volume) OVER (PARTITION BY stock_symbol ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_total_volume
                FROM {production_table}
                WHERE trade_date >= DATE('{{{{ ds }}}}') - INTERVAL '6' DAY
            """
        }
    )

    create_staging_table >> load_to_staging >> run_dq_check >> create_production_table
    create_production_table >> insert_into_production >> drop_staging_table
    drop_staging_table >> calculate_rolling_avg

stock_data_pipeline()
