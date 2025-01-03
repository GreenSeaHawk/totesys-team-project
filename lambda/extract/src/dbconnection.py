import pg8000
import boto3
import json
from botocore.exceptions import ClientError
from pg8000 import DatabaseError


def get_db_credentials(secret_name="my-database-connection"):
    """get the credentials from the secret manager"""
    my_client = boto3.client("secretsmanager", region_name="eu-west-2")
    try:
        response = my_client.get_secret_value(SecretId=secret_name)
        secret = response["SecretString"]  # will have all the credentials
        credentials = json.loads(secret)
        return {
            "cohort_id": credentials["cohort_id"],
            "user": credentials["user"],
            "password": credentials["password"],
            "host": credentials["host"],
            "database": credentials["database"],
            "port": credentials["port"],
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            raise Exception("The Secret name could not be found.") from e


def connect_to_db(secret_name="my-database-connection"):
    credentials = get_db_credentials(secret_name)
    try:
        conn = pg8000.connect(
            user=credentials["user"],
            password=credentials["password"],
            host=credentials["host"],
            database=credentials["database"],
            port=credentials["port"],
        )
        return conn
    except pg8000.DatabaseError as de:
        error_message = "Some database connectivity error occurred."
        raise DatabaseError(error_message) from de


def close_db_connection(conn):
    conn.close()
