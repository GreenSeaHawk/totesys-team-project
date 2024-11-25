import pytest
from unittest.mock import patch
from src.write_transfomed_data_to_s3 import write_transformed_data_to_s3


@patch("src.write_transfomed_data_to_s3.add_to_s3_file_parquet")
def test_write_transformed_data_to_s3_success(
    mock_add_to_s3_file_parquet, caplog
):
    # mock successful S3 upload
    mock_add_to_s3_file_parquet.return_value = None

    # test data
    tranform_bucket = "test-transform-bucket"
    table_name = "test-table"
    tranformed_data = [{"id": 1, "name": "Charlie"}]

    # call the function
    with caplog.at_level("INFO"):
        write_transformed_data_to_s3(
            transform_bucket=tranform_bucket,
            table_name=table_name,
            transformed_data=tranformed_data,
        )
    # Assertions
    assert (
        f"Writing transforming data for table {table_name} to S3"
        in caplog.text
    )
    assert (
        f"Transformation completed sucessfully for table {table_name}."
        in caplog.text
    )


@patch("src.write_transfomed_data_to_s3.add_to_s3_file_parquet")
def test_write_transformed_data_to_s3_logs_failure(
    mock_add_to_s3_file_parquet, caplog
):
    # mock successful S3 failure
    mock_add_to_s3_file_parquet.side_effect = Exception("Mock S3 Failure.")

    # test data
    tranform_bucket = "test-transform-bucket"
    table_name = "test-table"
    tranformed_data = [{"id": 1, "name": "Charlie"}]

    # call the function and expect an exception
    with pytest.raises(Exception, match="Mock S3 Failure."):
        with caplog.at_level("INFO"):
            write_transformed_data_to_s3(
                transform_bucket=tranform_bucket,
                table_name=table_name,
                transformed_data=tranformed_data,
            )
    # Assertions
    assert (
        f"Writing transforming data for table {table_name} to S3"
        in caplog.text
    )
    assert (
        f"Failed to write transformed data for table {table_name}"
        f" to s3: Mock S3 Failure." in caplog.text
    )
