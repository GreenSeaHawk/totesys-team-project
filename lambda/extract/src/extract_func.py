import json
import pandas as pd


def extract_table_data(db_conn, table_name, last_ran):
    """extracting data from a specified table and returning it as a dataframe"""
    query = f"SELECT * FROM {table_name} WHERE created_at > '{last_ran}' OR last_updated > '{last_ran}'"
    return pd.read_sql(query,db_conn)

def serialise_data(data, format='json'):
    """converts data into a serialised format: json, csv, parquet"""
    if format == 'json':
        return data.to_json(orient="records")
    elif format == "csv":
        return data.to_csv(index=False)
    elif format == 'parquet':
        return data.to_parquet(index=False)
    else:
        raise ValueError('unsupported format choose from either json, csv, or parquet')