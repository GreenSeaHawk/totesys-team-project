import pg8000
import boto3
import json
from pprint import pprint
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError
from pg8000 import DatabaseError

def get_db_credentials(secret_name = "my-database-connection"):
    """get the credentials from the secret manager"""
    my_client = boto3.client("secretsmanager", region_name="eu-west-2")
    
    try:
        response = my_client.get_secret_value(SecretId=secret_name)
        secret = response['SecretString'] # will have all the credentials
        credentials = json.loads(secret)
        return {
            "cohort_id": credentials["cohort_id"], 
            "user": credentials["user"], 
            "password": credentials["password"], 
            "host": credentials["host"], 
            "database": credentials["database"], 
            "port": credentials["port"]
        }
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise Exception("The Secret name could not be found, please check.") from e

def connect_to_db(secret_name = "my-database-connection"):
    credentials = get_db_credentials(secret_name)
    try:
        conn = pg8000.connect( user= credentials["user"], 
            password = credentials["password"], 
            host= credentials["host"], 
            database= credentials["database"], 
            port= credentials["port"])
       
        return conn
    except pg8000.DatabaseError as de:
        error_message = "Unfortunately, some database connectivity error occurred."
        raise DatabaseError(error_message) from de

def close_db_connection(conn):
    conn.close()

def get_data_from_table(table_name):
    query = f'''SELECT * FROM {table_name}'''
    conn = connect_to_db()
    response=conn.run(query)
    columns = [c[0] for c in conn.description]
    formatted_response =[]
    for row in response:
        formatted_response.append(dict(zip(columns, row)))
    
    for row in formatted_response:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = value.isoformat()
            elif isinstance(value, Decimal):
                row[key] = float(value)
   
    with open(f"data/{table_name}.json", "w") as json_file:
        for item in formatted_response:
            json_line = json.dumps(item)
            json_file.write(json_line + "\n")


get_data_from_table("department")