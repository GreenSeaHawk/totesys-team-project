# successful execution = data is processed and uploaded correctly
# database connection can fail
# data extraction can fail
# data uploadation can fail
# database connection gets closed no matter what

import pytest
import boto3
from src.handler import lambda_handler
from unittest.mock import patch, MagicMock
from moto import mock_s3
from botocore.exceptions import ClientError

# Test constants
BUCKET_NAME = "totesys-data-bucket-cimmeria"
TABLES = ["A", "B", "C", "D"]


@pytest.fixture
def setup_s3_bucket():
    """Mock the S3 bucket for testing with moto."""
    with mock_s3():
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


@patch("src.handler.update_last_ran_s3")
@patch("src.handler.get_last_ran")
@patch("src.handler.upload_raw_data_to_s3")
@patch("src.handler.serialise_data")
@patch("src.handler.extract_table_data")
@patch("src.handler.connect_to_db")
def test_lambda_handler_success(
    mock_connect_to_db,
    mock_extract_table_data,
    mock_serialise_data,
    mock_upload_raw_data_to_s3,
    mock_get_last_ran,
    mock_update_last_ran_s3,
    setup_s3_bucket,
    monkeypatch,
):
    monkeypatch.setenv("BUCKET_NAME", BUCKET_NAME)
    monkeypatch.setattr("src.handler.TABLES", TABLES)
    # Mock the database connection
    mock_db_conn = MagicMock()
    mock_connect_to_db.return_value = mock_db_conn

    # Mock get_last_ran to return a specific timestamp
    mock_get_last_ran.return_value = "1900-01-01T00:00:00"

    # Mock extract_table_data to return a non-empty DataFrame for each table
    mock_data = MagicMock()
    mock_data.empty = False
    mock_extract_table_data.return_value = mock_data

    # Mock serialise_data to return JSON-formatted data
    mock_serialise_data.return_value = '{"id":1, "name":"Charlie"}'

    # Call the lambda_handler
    result = lambda_handler({}, {})

    # Assertions
    mock_connect_to_db.assert_called_once()
    mock_get_last_ran.assert_called_once_with(BUCKET_NAME)
    assert mock_extract_table_data.call_count == len(TABLES)
    mock_serialise_data.assert_called()
    assert mock_upload_raw_data_to_s3.call_count == len(TABLES)
    mock_update_last_ran_s3.assert_called_once_with(BUCKET_NAME)

    # Verify the Lambda success response
    assert result["statusCode"] == 200
    assert "Data Ingestion completed successfully." in result["body"]


@patch("src.handler.connect_to_db")
def test_lambda_handlder_db_connection_failure(
    mock_connect_to_db, monkeypatch
):
    monkeypatch.setenv("BUCKET_NAME", BUCKET_NAME)
    monkeypatch.setattr("src.handler.TABLES", TABLES)

    result = lambda_handler({}, {})
    assert result["statusCode"] == 500
    assert "Unfortunately, the lambda function failed." in result["body"]

    mock_connect_to_db.assert_called_once()


# data extraction failure
@patch("src.handler.get_last_ran")
@patch(
    "src.handler.extract_table_data",
    side_effect=Exception("Data extraction failed."),
)
@patch("src.handler.connect_to_db")
def test_lambda_handlder_data_extraction_failure(
    mock_connect_to_db,
    mock_extract_table_data,
    mock_get_last_ran,
    setup_s3_bucket,
    monkeypatch,
):
    monkeypatch.setenv("BUCKET_NAME", BUCKET_NAME)
    monkeypatch.setattr("src.handler.TABLES", TABLES)

    mock_db_conn = MagicMock()
    mock_connect_to_db.return_value = mock_db_conn

    mock_get_last_ran.return_value = "1900-01-01T00:00:00"

    result = lambda_handler({}, {})
    assert result["statusCode"] == 500
    assert "Unfortunately, the lambda function failed." in result["body"]

    mock_connect_to_db.assert_called_once()
    mock_get_last_ran.assert_called_once_with(BUCKET_NAME)


@patch("src.handler.get_last_ran")
@patch(
    "src.handler.upload_raw_data_to_s3",
    side_effect=ClientError(
        error_response={
            "Error": {"Code": "500", "Message": "S3 upload failed"}
        },
        operation_name="PutObject",
    ),
)
@patch("src.handler.serialise_data")
@patch("src.handler.extract_table_data")
@patch("src.handler.connect_to_db")
def test_lambda_handlder_s3_upload_failure(
    mock_connect_to_db,
    mock_extract_table_data,
    mock_serialise_data,
    mock_upload_raw_data_to_s3,
    mock_get_last_ran,
    setup_s3_bucket,
    monkeypatch,
):
    monkeypatch.setenv("BUCKET_NAME", BUCKET_NAME)
    monkeypatch.setattr("src.handler.TABLES", TABLES)

    mock_db_conn = MagicMock()
    mock_connect_to_db.return_value = mock_db_conn

    mock_get_last_ran.return_value = "1900-01-01T00:00:00"

    mock_data = MagicMock()
    mock_data.empty = False
    mock_extract_table_data.return_value = mock_data

    mock_serialise_data.return_value = '{"id":1, "name":"Charlie"}'
    result = lambda_handler({}, {})
    assert result["statusCode"] == 500
    assert "Unfortunately, the lambda function failed." in result["body"]

    mock_connect_to_db.assert_called_once()
    mock_get_last_ran.assert_called_once_with(BUCKET_NAME)
    mock_upload_raw_data_to_s3.assert_called()


@patch("src.handler.close_db_connection")
@patch("src.handler.get_last_ran")
@patch("src.handler.connect_to_db")
def test_lambda_handler_closes_db_connection(
    mock_connect_to_db,
    mock_get_last_ran,
    mock_close_db_connection,
    setup_s3_bucket,
    monkeypatch,
):
    monkeypatch.setenv("BUCKET_NAME", BUCKET_NAME)
    monkeypatch.setattr("src.handler.TABLES", TABLES)

    mock_db_conn = MagicMock()
    mock_connect_to_db.return_value = mock_db_conn

    mock_get_last_ran.return_value = "1900-01-01T00:00:00"

    lambda_handler({}, {})
    mock_connect_to_db.assert_called_once()
    mock_close_db_connection.assert_called_once_with(mock_db_conn)

