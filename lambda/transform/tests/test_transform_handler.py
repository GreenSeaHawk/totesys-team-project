import pytest
from unittest.mock import patch, MagicMock
from src.transform_handler import get_data_from_files, add_to_s3_file_json, add_to_s3_file_parquet, list_all_filenames_in_s3, lambda_handler

@pytest.fixture
def mock_event():
    """Create a mock lambda event for the test"""
    return {
        "source_bucket" : 'ingestion_bucket',
        "transform_bucket" : 'transform_bucket',
        "prefix" : 'payment_type',
        "last_run_key" : 'last_run.json'
    }

@pytest.fixture
def mock_context():
     """Create a mock context event for the test"""
     return None
    
@patch("src.transform_handler.list_all_filenames_in_s3")
@patch("src.transform_handler.get_data_from_files")
@patch("src.transform_handler.add_to_s3_file_parquet")
def test_lambda_handler_success(
    mock_add_to_s3_file_parquet,
    mock_get_data_from_files,
    mock_list_all_filenames_in_s3,
    mock_event,
    mock_context
):
    mock_list_all_filenames_in_s3.return_value = [
        "payment_type/2023/01/payment_type_20230101000000.json"
    ]
    mock_get_data_from_files.return_value = [
        {"payment_type_id":4, "payment_type_name":"card"}
    ]
    mock_add_to_s3_file_parquet.return_value = None # for simulating successful upload

    # Call the lambda handler
    result = lambda_handler(mock_event, mock_context)

    #Assertions
    mock_list_all_filenames_in_s3.assert_called_once_with(
        Bucket='ingestion_bucket', prefix='payment_type', Key='last_run.json'
    )

    mock_get_data_from_files.assert_called_once_with(
        Bucket='ingestion_bucket',
        list_of_files=['payment_type/2023/01/payment_type_20230101000000.json']
    )

    mock_add_to_s3_file_parquet.assert_called_once_with(
        bucket='transform_bucket',
        data=[{"payment_type_id":4, "payment_type_name":"card"}],
        table='payment_type'
    )

    assert result['statusCode'] == 200
    assert 'Transformation went fine.' in result['body']


@patch("src.transform_handler.list_all_filenames_in_s3")
@patch("src.transform_handler.get_data_from_files")
@patch("src.transform_handler.add_to_s3_file_parquet")
def test_lambda_handler_no_files_to_transform(
    mock_add_to_s3_file_parquet,
    mock_get_data_from_files,
    mock_list_all_filenames_in_s3,
    mock_event,
    mock_context
):

    mock_list_all_filenames_in_s3.return_value = [] # no files found
    mock_get_data_from_files.return_value = []
    mock_add_to_s3_file_parquet.return_value = None
    
    # Call the lambda handler
    result = lambda_handler(mock_event, mock_context)

    #Assertions
    mock_list_all_filenames_in_s3.assert_called_once_with(
        Bucket='ingestion_bucket', prefix='payment_type', Key='last_run.json'
    )

    mock_get_data_from_files.assert_not_called()

    mock_add_to_s3_file_parquet.assert_not_called()

    assert result['statusCode'] == 200
    assert 'Processed 0 files.' in result['body']

@patch("src.transform_handler.list_all_filenames_in_s3")
def test_lambda_handler_missing_last_key(
    mock_list_all_filenames_in_s3,
    mock_event,
    mock_context
):

    mock_list_all_filenames_in_s3.side_effect = Exception(
        "Error retrieving last_run.json: NoSuchKey"
    )
    
    # Call the lambda handler
    result = lambda_handler(mock_event, mock_context)

    #Assertions
    mock_list_all_filenames_in_s3.assert_called_once_with(
        Bucket='ingestion_bucket', prefix='payment_type', Key='last_run.json'
    )

    assert result['statusCode'] == 500
    assert 'An unexpected Error occured. Error retrieving last_run.json' in result['body']