import pandas as pd
from datetime import datetime
import boto3

def insert_data_to_postgres(df, table_name, engine):
    """Insert a DataFrame into a PostgreSQL table."""
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Inserted {len(df)} rows into {table_name}.")


def update_last_ran_s3(bucket_name, Key):
    """after processing update the last_ran file
    in s3 with the current timestamp"""
    s3_client = boto3.client("s3")
    current_time = datetime.now().isoformat()
    s3_client.put_object(
        Bucket=bucket_name, Key=Key, Body=current_time
    )
