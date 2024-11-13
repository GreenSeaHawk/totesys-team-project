import pytest
from botocore.exceptions import ClientError
from unittest.mock import patch, MagicMock
from src.upload_data_s3 import upload_raw_data_to_s3, get_last_ran, update_last_ran_s3
from moto import mock_s3
import boto3
from datetime import datetime

# TEST parameters
BUCKET_NAME = "test_bucket"
DATA = "{'sample_key':'sample_value'}"
TABLE_NAME ="sample_table"
LAST_RAN_KEY = 'last_ran.txt'

@pytest.fixture
def setup_s3_bucket():
    """Mock the S3 bucket for testing with moto."""
    with mock_s3():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        yield s3 

def test_upload_to_s3_success(setup_s3_bucket):
    # call the function
    s3_key = upload_raw_data_to_s3(BUCKET_NAME, DATA, TABLE_NAME)

    # verify the file was uploaded to the correct location put_object was called with corrrect parameters
    response  = setup_s3_bucket.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    assert response["Body"].read().decode("utf-8") == DATA

def test_upload_to_s3_success_with_correct_folder_structure(setup_s3_bucket):
    s3_key = upload_raw_data_to_s3(BUCKET_NAME, DATA, TABLE_NAME)

    # expected folder structure has been created successfully.
    assert s3_key.startswith(f"{TABLE_NAME}/")
    assert s3_key.endswith(".json")

@patch("src.upload_data_s3.boto3.client")
def test_upload_to_s3_failure(mock_s3_client):
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = ClientError(
        error_response = {"Error":{"Code":500, "Message":"Internal Server Error"}},
        operation_name = 'PutObject'
    )
    mock_s3_client.return_value = mock_s3

    #call the function, and check for the raised exception
    with pytest.raises(Exception, match="Failed to upload data to s3"):
        upload_raw_data_to_s3(BUCKET_NAME, DATA, TABLE_NAME)


def test_get_last_ran_on_existing_file(setup_s3_bucket):
    last_ran_time = datetime(2023, 1,1,12,0,0).isoformat()
    setup_s3_bucket.put_object(Bucket=BUCKET_NAME, Key= LAST_RAN_KEY, Body=last_ran_time)
    result = get_last_ran(BUCKET_NAME)
    assert result == datetime.fromisoformat(last_ran_time)

def test_get_last_ran_when_no_file(setup_s3_bucket):
    result = get_last_ran(BUCKET_NAME)
    assert result == datetime(1900,1,1)
    
def test_update_last_ran_puts_current_time(setup_s3_bucket):
    update_last_ran_s3(BUCKET_NAME)
    response = setup_s3_bucket.get_object(Bucket=BUCKET_NAME, Key= LAST_RAN_KEY)
    stored_time = response['Body'].read().decode('utf-8')
    current_time = datetime.fromisoformat(stored_time)
    assert (datetime.now()-current_time).total_seconds() < 3

    
# @patch("src.upload_data_s3.boto3.client")
# def test_upload_to_s3_success(mock_s3_client):
#     # Mock the S3 client's put_object method
#     mock_s3 = MagicMock()
#     mock_s3_client.return_value = mock_s3

#     # test paramters
#     bucket_name = "test_bucket"
#     data = "{'sample_key':'sample_value'}"
#     table_name ="sample_table"

#     # call the function
#     s3_key = upload_raw_data_to_s3(bucket_name, data, table_name)

#     # verify put_object was called with corrrect parameters
#     mock_s3.put_object.assert_called_once_with(Bucket=bucket_name, Key=s3_key, Body=data, ContentType='application/json')