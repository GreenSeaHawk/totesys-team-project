# from pg8000.native import Connection
import pg8000
import boto3
import json
from botocore.exceptions import ClientError

def get_db_credentials(secret_name = "my-database-connection"):
    """get the credentials from the secret manager"""
    my_client = boto3.client("secretsmanager", region_name="eu-west-2")
    # secret_name = "my-database-connection"
    try:
        response = my_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return 'The Secret name could not be found, please check.'
    secret = response['SecretString'] # will have all the credentials
    credentials = json.loads(secret)
    return {"cohort_id": credentials["cohort_id"], 
    "user": credentials["user"], 
    "password": credentials["password"], 
    "host": credentials["host"], 
    "database": credentials["database"], 
    "port": credentials["port"]}

def connect_to_db():
    credentials = get_db_credentials()
    if not credentials:
        print("Failed to retrieve database credentials.")
    try:
        conn = pg8000.connect( user= credentials["user"], 
            password = credentials["password"], 
            host= credentials["host"], 
            database= credentials["database"], 
            port= credentials["port"])
        print("Database connection successful....")
        return conn
    except pg8000.DatabaseError as de:
        raise de

def close_db_connection(conn):
    conn.close()

def get_secret():

    secret_name = "my-database-connection"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']

    if "SecretString" in get_secret_value_response:
        secret = get_secret_value_response["SecretString"]
        secret_dict = json.loads(secret) # json string to dictionary
        return secret_dict
    else:
        pass

# credentials = get_secret()
# if credentials:
#     print("Database User", credentials["user"]) # e.g. username
#     print("Database Password", credentials["password"])
#     print("Host", credentials["host"])
#     print("Database Name", credentials["database"])
#     print("Database Port", credentials["port"])
