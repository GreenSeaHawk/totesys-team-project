import boto3
import json
from botocore.exceptions import ClientError
import sqlalchemy


def get_db_credentials(secret_name="my-database-connection"):
    """get the credentials from the secret manager"""
    my_client = boto3.client("secretsmanager", region_name="eu-west-2")
    try:
        response = my_client.get_secret_value(SecretId=secret_name)
        secret = response["SecretString"]  # will have all the credentials
        credentials = json.loads(secret)
        return {
            "user": credentials["user"],
            "password": credentials["password"],
            "host": credentials["host"],
            "database": credentials["database"],
            "port": credentials["port"],
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            raise Exception("The Secret name could not be found.") from e


def return_engine(db_credentials):
    """Create an sqlalchemy engine from Database credentials"""
    engine = sqlalchemy.create_engine(
        (f'postgresql://{db_credentials["user"]}:{db_credentials["password"]}'
         f'@{db_credentials["host"]}:{db_credentials["port"]}/'
         f'{db_credentials["database"]}')
    )
    return engine
