import pyarrow as pa
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

from aws_secret_manager import get_secret
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    LongType,
    DoubleType,
    StringType,
    NestedField
)
from dotenv import load_dotenv
from ast import literal_eval
import requests
from datetime import datetime

load_dotenv()

def fetch_stock_data(ticker, api_key):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2025-01-05/2025-01-06?adjusted=true&sort=asc&apiKey={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data.get('results', [])
    else:
        print(f"Failed to fetch data for {ticker}: {response.status_code}")
        return []

def convert_to_pyarrow(data, ticker):
    rows = [
        {
            "ticker": ticker,
            "timestamp": datetime.utcfromtimestamp(item['t'] / 1000),  # Convert milliseconds to seconds
            "open_price": item['o'],
            "high_price": item['h'],
            "low_price": item['l'],
            "close_price": item['c'],
            "volume": item['v']
        }
        for item in data
    ]
    return pa.Table.from_pylist(rows)

def homework_script():
    maang_stocks = ['AAPL', 'AMZN', 'NFLX', 'GOOGL', 'META']
    polygon_api_key = literal_eval(get_secret("POLYGON_CREDENTIALS"))['AWS_SECRET_ACCESS_KEY']
    catalog = load_catalog(
        'academy',
        type="rest",
        uri="https://api.tabular.io/ws",
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret('TABULAR_CREDENTIAL')
    )

    table_identifier = "anushapamalidsilva24909.maang_stock"

    # Define table properties
    schema = Schema(
        NestedField(field_id=1, name="ticker", field_type=StringType(), required=False),
        NestedField(field_id=2, name="timestamp", field_type=TimestampType(), required=False),
        NestedField(field_id=3, name="open_price", field_type=DoubleType(), required=False),
        NestedField(field_id=4, name="high_price", field_type=DoubleType(), required=False),
        NestedField(field_id=5, name="low_price", field_type=DoubleType(), required=False),
        NestedField(field_id=6, name="close_price", field_type=DoubleType(), required=False),
        NestedField(field_id=7, name="volume", field_type=LongType(), required=False)
    )
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=1, field_id=1000, transform=DayTransform(), name="day"))

    # Step 1: Create the Iceberg table
    try:
        print(f"Checking if table '{table_identifier}' exists...")
        if catalog.table_exists(table_identifier):
            print(f"Table '{table_identifier}' already exists. Skipping creation.")
        else:
            catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec
            )
            print(f"Table '{table_identifier}' created successfully.")
    except Exception as e:
        print(f"Error during table creation or existence check: {e}")

    # Step 2: Load data from API into the Iceberg table
    try:
        table = catalog.load_table(table_identifier)
        for ticker in maang_stocks:
            print(f"Fetching data for {ticker}...")
            stock_data = fetch_stock_data(ticker, polygon_api_key)
            if stock_data:
                print(f"Loading data for {ticker} into the Iceberg table...")
                pyarrow_table = convert_to_pyarrow(stock_data, ticker)
                table.append(pyarrow_table)
                print(f"Data for {ticker} loaded successfully.")
            else:
                print(f"No data found for {ticker}.")
    except Exception as e:
        print(f"Error during data load: {e}")

    # Step 3: Create a branch
    try:
        if not list(table.snapshots()):
            empty_data = pa.Table.from_pylist([])  # No-op write for initial snapshot
            table.append(empty_data)

        current_snapshot = table.current_snapshot().snapshot_id
        branch_manager = table.manage_snapshots()
        (
            branch_manager
            .create_branch(snapshot_id=current_snapshot, branch_name='audit_branch')
            .create_tag(snapshot_id=current_snapshot, tag_name="Creating audit_branch")
            .commit()
        )
        print("Branch 'audit_branch' created successfully.")
    except Exception as e:
        print(f"Error creating branch: {e}")


if __name__ == "__main__":
    homework_script()
