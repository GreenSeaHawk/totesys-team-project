import pytest
import json
import boto3
from src.dbconnection import get_db_credentials, connect_to_db
# from moto.secretsmanager import mock_secretsmanager
from moto import mock_aws
from pg8000 import DatabaseError
from unittest.mock import patch, MagicMock


# Initialize mock Secrets Manager
@pytest.fixture
def secrets_client():
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


@mock_secretsmanager
def test_get_db_credentials_success(secrets_client):
    secret_name = "my-database-connection"
    secret_value = {
        "cohort_id": 1,
        "user": "test_user",
        "password": "test_pass",
        "host": "my_host",
        "database": "my_database",
        "port": 1000,
    }
    secrets_client.create_secret(
        Name=secret_name, SecretString=json.dumps(secret_value)
    )

    # Act
    result = get_db_credentials(secret_name="my-database-connection")

    # Assert
    assert result == secret_value
    assert result["user"] == "test_user"
    assert result["password"] == "test_pass"
    assert result["database"] == "my_database"
    assert result["host"] == "my_host"
    assert result["port"] == 1000
    assert result["cohort_id"] == 1


@mock_secretsmanager
def test_get_db_credentials_with_secret_value_not_found(secrets_client):
    secret_name = "my-database-connection"
    secret_value = {
        "cohort_id": 1,
        "user": "test_user",
        "password": "test_pass",
        "host": "my_host",
        "database": "my_database",
        "port": 1000,
    }
    secrets_client.create_secret(
        Name=secret_name, SecretString=json.dumps(secret_value)
    )
    # Assert
    with pytest.raises(Exception, match="The Secret name could not be found."):
        get_db_credentials(secret_name="my-database-connection1")


@patch("src.dbconnection.get_db_credentials")
@patch("src.dbconnection.pg8000.connect")
def test_connect_to_db_success(mock_connect, mock_get_db_credentials):
    mock_get_db_credentials.return_value = {
        "user": "test_user",
        "password": "test_password",
        "host": "test_host",
        "database": "test_database",
        "port": 1000,
    }
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    conn = connect_to_db()
    mock_connect.assert_called_once_with(
        user="test_user",
        password="test_password",
        host="test_host",
        database="test_database",
        port=1000,
    )
    assert conn == mock_conn


def test_connect_to_db_when_get_credentials_is_invalid():
    invalid_cred = {
        "user": "invalid",
        "password": "invalid",
        "host": "invalid",
        "database": "invalid",
        "port": 1000,
    }
    with patch(
        "src.dbconnection.get_db_credentials", return_value=invalid_cred
    ), patch(
        "src.dbconnection.pg8000.connect",
        side_effect=DatabaseError("invalid_cred"),
    ):
        with pytest.raises(DatabaseError) as e:
            connect_to_db()
        assert "Some database connectivity error occurred." in str(e.value)


@patch("src.dbconnection.get_db_credentials")
@patch("src.dbconnection.pg8000.connect")
def test_connect_to_db_when_database_error(
    mock_connect, mock_get_db_credentials, capsys
):
    mock_get_db_credentials.return_value = {
        "user": "test_user",
        "password": "test_password",
        "host": "test_host",
        "database": "test_database",
        "port": 1000,
    }
    mock_connect.side_effect = DatabaseError("Database connection failed")
    with pytest.raises(
        DatabaseError,
        match="Some database connectivity error occurred.",
    ):

        connect_to_db()

