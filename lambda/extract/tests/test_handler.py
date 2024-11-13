# successful execution = data is processed and uploaded correctly
# database connection can fail
#data extraction can fail
# data uploadation can fail
# database connection gets closed no matter what

import pytest
import boto3
from src.handler import lambda_handler
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from moto import mock_s3


BUCKET_NAME1 = "test_bucket"
TABLES = ["A", "B", "C", "D"]
MOCK_CREDENTIALS = {
            'user': "test_user", 
            'password' : "test_password", 
            'host': "test_host", 
            'database' : "test_database", 
            'port' : 2000
    }

@pytest.fixture
def setup_s3_bucket():
    """Mock the S3 bucket for testing with moto."""
    with mock_s3():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET_NAME1, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        yield s3 

@patch("src.upload_data_s3.update_last_ran_s3")
@patch("src.upload_data_s3.get_last_ran")
@patch("src.upload_data_s3.upload_raw_data_to_s3")
@patch("src.extract_func.serialise_data")
@patch("src.extract_func.extract_table_data")
@patch("src.dbconnection.connect_to_db")
@patch("src.dbconnection.get_db_credentials")
def test_lambda_handler_success(mock_get_db_credentials, mock_connect_to_db, mock_extract_table_data, mock_serialise_data, mock_upload_data_to_s3, mock_get_last_ran, mock_update_last_ran_s3, setup_s3_bucket):
    # mock database connection
    mock_get_db_credentials.return_value = MOCK_CREDENTIALS
    mock_conn = MagicMock()
    
    mock_connect_to_db.return_value = mock_conn
    #mock_db_conn = MagicMock()
    #mock_connect_to_db.return_value = mock_db_conn

    # mock get_last_ran to a default timestamp
    mock_get_last_ran.return_value = "1900-01-01T00:00:00"

    #mock extract_table_data and return a NON-EMPTY dataframe for each table
    mock_data = MagicMock()
    mock_data.empty = False
    mock_extract_table_data.return_value = mock_data

    # mock serialise data to return Json data
    mock_serialise_data.return_value = "{'id':1, 'name':'Charlie'}"

    # call the lambda handler
    result = lambda_handler({}, {})

    #Assertions
    mock_connect_to_db.assert_called_once()
    mock_get_last_ran.assert_called_once_with(BUCKET_NAME1)
    assert mock_extract_table_data.call_count == len(TABLES)
    mock_serialise_data.assert_called()
    assert mock_upload_data_to_s3.call_count == len(TABLES)
    mock_update_last_ran_s3.assert_called_once_with(BUCKET_NAME1)

    # verify once all gets complete
    assert result['statusCode'] == 200
    assert "Data Ingestion completed successfully." in result['body']