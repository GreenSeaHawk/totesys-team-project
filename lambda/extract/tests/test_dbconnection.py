import pytest
import json
import boto3
from src.dbconnection import get_db_credentials, connect_to_db
from moto.secretsmanager import mock_secretsmanager
from botocore.exceptions import ClientError
from pg8000 import DatabaseError
from unittest.mock import patch, MagicMock

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


@patch("src.dbconnection.get_db_credentials")
@patch("src.dbconnection.pg8000.connect")
def test_connect_to_db_success(mock_connect, mock_get_db_credentials):
    mock_get_db_credentials.return_value = {
            'user': "test_user", 
            'password' : "test_password", 
            'host': "test_host", 
            'database' : "test_database", 
            'port' : 1000
    }
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    conn = connect_to_db()
    mock_connect.assert_called_once_with(user= "test_user", 
            password = "test_password", 
            host= "test_host", 
            database = "test_database", 
            port = 1000)
    assert conn == mock_conn

@patch("src.dbconnection.get_db_credentials")
@patch("src.dbconnection.pg8000.connect")
def test_connect_to_db_when_no_credentials(mock_connect, mock_get_db_credentials, capsys):
    mock_get_db_credentials.return_value = None
    conn = connect_to_db()
    captured = capsys.readouterr()
    assert conn is None
    assert "Failed to retrieve database credentials." in captured.out
    mock_connect.assert_not_called()

@patch("src.dbconnection.get_db_credentials")
@patch("src.dbconnection.pg8000.connect")
def test_connect_to_db_when_database_error(mock_connect, mock_get_db_credentials, capsys):
    mock_get_db_credentials.return_value = {
            'user': "test_user", 
            'password' : "test_password", 
            'host': "test_host", 
            'database' : "test_database", 
            'port' : 1000
    }
    mock_connect.side_effect=DatabaseError('Database connection failed')
    with pytest.raises(DatabaseError, match="Database connection failed"):
        connect_to_db()