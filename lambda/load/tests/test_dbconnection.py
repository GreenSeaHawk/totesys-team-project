import pytest
import json
import boto3, sqlalchemy
from src.dbconnection import get_db_credentials, return_engine
from moto import mock_aws
from unittest.mock import patch, MagicMock


# Initialize mock Secrets Manager
@pytest.fixture
def secrets_client():
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


@pytest.fixture
def datbase_credentials(secrets_client):
    secret_name = "my-database-connection"
    secret_value = {
        "user": "test_user",
        "password": "test_pass",
        "host": "my_host",
        "database": "my_database",
        "port": 1000,
    }
    secrets_client.create_secret(
        Name=secret_name, SecretString=json.dumps(secret_value)
    )


class TestGetDBCredentials:
    def test_get_db_credentials_success(self, datbase_credentials):
        expected = {
            "user": "test_user",
            "password": "test_pass",
            "host": "my_host",
            "database": "my_database",
            "port": 1000,
        }
        # Act
        result = get_db_credentials(secret_name="my-database-connection")

        # Assert
        assert result == expected

    def test_get_db_credentials_with_secret_value_not_found(self):
        # Assert
        with pytest.raises(
            Exception, match="The Secret name could not be found."
        ):
            get_db_credentials(secret_name="my-database-connection1")


class TestReturnEngine:
    def test_returns_an_engine(self):
        credentials = {
            "user": "test_user",
            "password": "test_pass",
            "host": "my_host",
            "database": "my_database",
            "port": 1000,
        }
        result = return_engine(credentials)
        assert isinstance(result, sqlalchemy.engine.base.Engine)

    @patch("dbconnection.sqlalchemy.create_engine")
    def test_correct_engine_is_returned(self, mock_create_engine):
        credentials = {
            "user": "test_user",
            "password": "test_pass",
            "host": "my_host",
            "database": "my_database",
            "port": 1000,
        }
        expected_argument = f'postgresql://{credentials["user"]}:{credentials["password"]}@{credentials["host"]}:{credentials["port"]}/{credentials["database"]}'
        returned_engine = return_engine(credentials)
        mock_create_engine.assert_called_once_with(expected_argument)
