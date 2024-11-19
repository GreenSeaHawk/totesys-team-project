# import pytest, boto3, os ,io, pg8000, json
# import pandas as pd
# from moto import mock_aws
# from src.dbconnection import get_db_credentials, connect_to_db
# from unittest.mock import patch, MagicMock


# @pytest.fixture
# def secrets_client():
#     with mock_aws():
#         yield boto3.client("secretsmanager", region_name="eu-west-2")


# @mock_aws
# def test_get_db_credentials_success(secrets_client):
#     secret_name = "test-connection"
#     secret_value = {
#         "cohort_id": 1,
#         "user": "test_user",
#         "password": "test_pass",
#         "host": "my_host",
#         "database": "my_database",
#         "port": 1000,
#     }
#     secrets_client.create_secret(
#         Name=secret_name, SecretString=json.dumps(secret_value)
#     )

# @pytest.fixture
# def conn(test_get_db_credentials_success):
#     return connect_to_db(secret_name="test-connection")



        
# @patch("src.dbconnection.connect_to_db")
# def test_connect_to_db_success(mock_connect):
#     mock_conn = MagicMock()
#     mock_connect.return_value = mock_conn

#     def run_side_effect(query, params):
#         if query == "INSERT INTO TABLE_NAME" and params == {"id": 1}:
#             return [{"id": 1, "name": "Alice"}]
#         else:
#             raise database error

#     mock_conn.run.side_effect = run_side_effect
    
   