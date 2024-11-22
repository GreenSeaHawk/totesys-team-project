import pytest
import re
from botocore.exceptions import ClientError
from unittest.mock import patch, MagicMock
from src.upload_data_s3 import (
    upload_raw_data_to_s3,
    get_last_ran,
    update_last_ran_s3,
    get_timestamp,
)
from moto import mock_aws
import boto3
from datetime import datetime
from freezegun import freeze_time

# TEST parameters
BUCKET_NAME = "test_bucket"
DATA = "{'sample_key':'sample_value'}"
TABLE_NAME = "sample_table"
LAST_RAN_KEY = "last_ran.json"


@pytest.fixture
def setup_s3_bucket():
    """Mock the S3 bucket for testing with moto."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


def test_upload_to_s3_success(setup_s3_bucket):
    # call the function
    s3_key = upload_raw_data_to_s3(BUCKET_NAME, DATA, TABLE_NAME)

    # verify the file was uploaded to the correct location put_object
    # was called with corrrect parameters
    response = setup_s3_bucket.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    assert response["Body"].read().decode("utf-8") == DATA


@freeze_time("2012-01-14")
def test_upload_to_s3_success_with_correct_folder_structure(setup_s3_bucket):
    s3_key = upload_raw_data_to_s3(BUCKET_NAME, DATA, TABLE_NAME)

    # expected folder structure has been created successfully.
    expected_key = "sample_table/2012/01/sample_table_201201140000000000.json"
    assert s3_key == expected_key


@patch("src.upload_data_s3.boto3.client")
def test_upload_to_s3_failure(mock_s3_client):
    mock_s3 = MagicMock()
    mock_s3.put_object.side_effect = ClientError(
        error_response={
            "Error": {"Code": 500, "Message": "Internal Server Error"}
        },
        operation_name="PutObject",
    )
    mock_s3_client.return_value = mock_s3

    # call the function, and check for the raised exception
    with pytest.raises(Exception, match="Failed to upload:"):
        upload_raw_data_to_s3(BUCKET_NAME, DATA, TABLE_NAME)


def test_get_last_ran_on_existing_file(setup_s3_bucket):
    last_ran_time = datetime(2023, 1, 1, 12, 0, 0).isoformat()
    setup_s3_bucket.put_object(
        Bucket=BUCKET_NAME, Key=LAST_RAN_KEY, Body=last_ran_time
    )
    result = get_last_ran(BUCKET_NAME)
    assert result == datetime.fromisoformat(last_ran_time)


def test_get_last_ran_when_no_file(setup_s3_bucket):
    result = get_last_ran(BUCKET_NAME)
    assert result == datetime(1900, 1, 1)


def test_update_last_ran_puts_current_time(setup_s3_bucket):
    update_last_ran_s3(BUCKET_NAME)
    response = setup_s3_bucket.get_object(Bucket=BUCKET_NAME, Key=LAST_RAN_KEY)
    stored_time = response["Body"].read().decode("utf-8")
    current_time = datetime.fromisoformat(stored_time)
    assert (datetime.now() - current_time).total_seconds() < 3


def test_timestamp_format():
    timestamp = get_timestamp()

    assert len(timestamp) == 18  # exactly 18 characters

    assert timestamp[-4].isdigit()

    # verify the entire format
    assert re.match(r"\d{14}\d{4}", timestamp)
