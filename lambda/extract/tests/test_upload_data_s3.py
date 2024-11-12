import pytest
from botocore.exceptions import ClientError
from unittest.mock import patch, MagicMock
from src.upload_data_s3 import upload_raw_data_to_s3
from moto import mock_s3
import boto3

# TEST parameters
BUCKET_NAME = "test_bucket"
DATA = "{'sample_key':'sample_value'}"
TABLE_NAME ="sample_table"

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





    