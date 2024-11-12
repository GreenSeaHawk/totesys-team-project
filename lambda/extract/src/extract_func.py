import pg8000
import json
import pandas as pd

def extract_table_data(db_conn, table_name):
    """extracting data from a specified table and returning it as a dataframe"""
    query = f"SELECT * FROM {table_name}"
    # result = db_conn(query)
    return pd.read_sql(query,db_conn)

def serialise_data(data, format='json'):
    """converts data into a serialised format: json, csv, parquet"""
    if format == 'json':
        return data.to_json()
    elif format == "csv":
        return data.to_csv(index=False)
    elif format == 'parquet':
        return data.to_parquet(index=False)
    else:
        raise ValueError('unsupported format choose from either json, csv, or parquet')