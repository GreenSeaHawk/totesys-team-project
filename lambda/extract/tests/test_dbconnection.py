import pytest
import json
import boto3
from src.dbconnection import get_db_credentials
from moto.secretsmanager import mock_secretsmanager
from botocore.exceptions import ClientError


# Initialize mock Secrets Manager
@pytest.fixture
def secrets_client():
    with mock_secretsmanager():
        yield boto3.client("secretsmanager", region_name='eu-west-2')

@mock_secretsmanager
def test_get_db_credentials_success(secrets_client):
    secret_name="my-database-connection"
    secret_value = {"cohort_id":1, "user": "test_user", "password": "test_pass", "host":"my_host", "database":"my_database", "port":1000}
    secrets_client.create_secret(
        Name=secret_name, SecretString=json.dumps(secret_value)
    )

    # Act
    result = get_db_credentials(secret_name="my-database-connection")

    # Assert
    assert result == secret_value
    assert result['user'] == "test_user"
    assert result['password'] == "test_pass"
    assert result['database'] == "my_database"
    assert result['host'] == "my_host"
    assert result['port'] == 1000
    assert result['cohort_id'] == 1

@mock_secretsmanager
def test_get_db_credentials_with_secret_value_not_found(secrets_client):
    secret_name="my-database-connection"
    secret_value = {"cohort_id":1, "user": "test_user", "password": "test_pass", "host":"my_host", "database":"my_database", "port":1000}
    secrets_client.create_secret(
        Name=secret_name, SecretString=json.dumps(secret_value)
    )
    # Act
    result = get_db_credentials(secret_name="my-database-connection1")

    # Assert
    assert result == "The Secret name could not be found, please check."