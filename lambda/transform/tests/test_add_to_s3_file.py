from src.add_to_s3_file import (
    add_to_s3_file_json,
    list_all_objects_in_bucket,
    add_to_s3_file_parquet,
)
import pytest
import boto3
import os
import time
import pandas as pd
import io
import concurrent.futures
from moto import mock_aws
from freezegun import freeze_time

# from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
from test_get_data_from_files import generate_mock_file_data


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """
    Return a mocked S3 client
    """
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture
def create_transform_bucket(s3):
    s3.create_bucket(
        Bucket="transform_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


class TestAddTos3File:
    def test_one_file_added(self, s3, create_transform_bucket):
        add_to_s3_file_parquet("transform_bucket", [{"key": 1}], "address")
        response = s3.list_objects_v2(
            Bucket="transform_bucket", Prefix="address"
        )
        assert len(response["Contents"]) == 1

    @freeze_time("2023-01-01")
    def test_file_added_have_correct_names(self, s3, create_transform_bucket):
        add_to_s3_file_parquet("transform_bucket", [{"key": 1}], "address")
        response = s3.list_objects_v2(
            Bucket="transform_bucket", Prefix="address"
        )
        assert (
            response["Contents"][0]["Key"]
            == "address/2023/01/address_20230101000000000000.parquet"
        )

    def test_multiple_files_added(self, s3, create_transform_bucket):
        add_to_s3_file_parquet("transform_bucket", [{"key": 1}], "address")
        add_to_s3_file_parquet("transform_bucket", [{"key": 2}], "address")
        response = s3.list_objects_v2(
            Bucket="transform_bucket", Prefix="address"
        )
        assert len(response["Contents"]) == 2

    @freeze_time("2023-01-01")
    def test_success_message(self, create_transform_bucket, capsys):
        add_to_s3_file_parquet("transform_bucket", [{"key": 1}], "address")
        captured = capsys.readouterr()
        expected_output = (
            "Object address/2023/01/address_20230101000000000000.parquet "
            "uploaded successfully to s3://transform_bucket."
        )
        assert captured.out.strip() == expected_output

    # we are uploading 100000 files to s3. Concurrent uploads?
    def test_add_to_s3_stress(self, s3, create_transform_bucket):
        NUM_FILES = 8
        RECORDS_PER_FILE = 100
        TABLE_NAME = "test_table"

        # Generate mock data for concurrent uploads
        def upload_file(index):
            mock_data = generate_mock_file_data(RECORDS_PER_FILE)
            add_to_s3_file_json(
                bucket="transform_bucket", data=mock_data, table=TABLE_NAME
            )

        # For SEQUENTIAL, uncomment the following code.
        # for i in range(NUM_FILES):
        #     mock_data = generate_mock_file_data(RECORDS_PER_FILE)
        #     add_to_s3_file(bucket="transform_bucket", data =mock_data,
        #  table=TABLE_NAME)

        # for concurrent uploads
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            my_futures = [
                executor.submit(upload_file, i) for i in range(NUM_FILES)
            ]
            concurrent.futures.wait(my_futures)

        # validate the results using pagination
        all_objects = list_all_objects_in_bucket(
            bucket_name="transform_bucket"
        )
        uploaded_files = len(all_objects)

        print(f"Number of files uploaded: {uploaded_files}")
        assert uploaded_files == NUM_FILES


def generate_large_dataset(num_rows, num_columns=5):
    """Generate a large dataset with specified number of rows and columns."""
    # returns a dataframe containing the specified number of rows and columms
    data = {
        f"col_{i}": [j for j in range(num_rows)] for i in range(num_columns)
    }
    return pd.DataFrame(data)


class TestParquetFileToS3:
    # successful upload- successful upload, check the contents.
    # empty data -upload an [] list and ensures it works as expected.
    # error handling - simulate a ClientError

    def test_add_to_s3_file_parquet_sucess(self, s3, create_transform_bucket):
        # Arrange
        TABLE_NAME = "test_table"
        DATA = [{"id": 1, "name": "Charlie"}, {"id": 2, "name": "Robert"}]

        # Act
        add_to_s3_file_parquet(
            bucket="transform_bucket", data=DATA, table=TABLE_NAME
        )

        # Assert
        # List all objects in the bucket
        objects = s3.list_objects_v2(Bucket="transform_bucket")
        assert "Contents" in objects
        assert len(objects["Contents"]) == 1

        # Validate the uploaded file
        s3_key = objects["Contents"][0]["Key"]
        response = s3.get_object(Bucket="transform_bucket", Key=s3_key)
        file_content = response["Body"].read()

        # Load the Parquet file into a DataFrame
        df = pd.read_parquet(io.BytesIO(file_content), engine="pyarrow")
        assert not df.empty
        assert len(df) == len(DATA)
        assert list(df.columns) == ["id", "name"]

    def test_add_to_s3_file_parquet_empty_data(
        self, s3, create_transform_bucket
    ):
        # Arrange
        TABLE_NAME = "test_table"
        DATA = []  # empty list

        # Act
        add_to_s3_file_parquet(
            bucket="transform_bucket", data=DATA, table=TABLE_NAME
        )

        # Assert
        # List all objects in the bucket
        objects = s3.list_objects_v2(Bucket="transform_bucket")
        assert "Contents" in objects
        assert len(objects["Contents"]) == 1

        # Validate the uploaded file
        s3_key = objects["Contents"][0]["Key"]
        response = s3.get_object(Bucket="transform_bucket", Key=s3_key)
        file_content = response["Body"].read()

        # Load the Parquet file into a DataFrame
        df = pd.read_parquet(io.BytesIO(file_content), engine="pyarrow")
        assert df.empty

    def test_add_to_s3_file_parquet_client_error(
        self, mocker, s3, create_transform_bucket
    ):
        # Arrange
        TABLE_NAME = "test_table"
        DATA = [{"id": 1, "name": "Charlie"}, {"id": 2, "name": "Robert"}]

        # Mock boto's3 put_object to raise a clienterror
        mock_s3_client = mocker.patch("boto3.client")
        mock_s3_client.return_value.put_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "AccessDenied", "Message": "Access Denied"}
            },
            operation_name="PutObject",
        )

        # Act and Asserrt
        with pytest.raises(Exception) as e:
            add_to_s3_file_parquet(
                bucket="transform_bucket", data=DATA, table=TABLE_NAME
            )

        assert "Failed to upload data" in str(e.value)
        assert "Access Denied" in str(e.value)

    def test_add_to_s3_file_stress_large_dataset(
        self, mocker, s3, create_transform_bucket
    ):

        TABLE_NAME = "test_table"
        # Generate a large dataset
        num_rows = 1_000_000
        data = generate_large_dataset(num_rows=num_rows)

        # Act
        start_time = time.time()
        add_to_s3_file_parquet(
            bucket="transform_bucket",
            data=data.to_dict(orient="records"),
            table=TABLE_NAME,
        )
        end_time = time.time()

        # Verify the uploaded file exists
        objects = s3.list_objects_v2(Bucket="transform_bucket")
        assert "Contents" in objects
        assert len(objects["Contents"]) == 1

        # Validate the uploaded file
        s3_key = objects["Contents"][0]["Key"]
        response = s3.get_object(Bucket="transform_bucket", Key=s3_key)
        file_content = response["Body"].read()
        uploaded_df = pd.read_parquet(
            io.BytesIO(file_content), engine="pyarrow"
        )

        assert not uploaded_df.empty
        assert len(uploaded_df) == num_rows
        print(
            f"Executing {num_rows} rows: {end_time - start_time:.2f} seconds."
        )

    def test_add_to_s3_file_stress_high_volume(
        self, s3, create_transform_bucket
    ):
        TABLE_NAME = "volume-table"
        NUM_FILES = 100
        NUM_ROWS_PER_FILE = 10_000

        start_time = time.time()
        for i in range(NUM_FILES):
            data = generate_large_dataset(NUM_ROWS_PER_FILE)
            add_to_s3_file_parquet(
                bucket="transform_bucket",
                data=data.to_dict(orient="records"),
                table=TABLE_NAME,
            )
        end_time = time.time()

        all_objects = s3.list_objects_v2(Bucket="transform_bucket")
        uploaded_files = len(all_objects.get("Contents", []))
        assert uploaded_files == NUM_FILES
        print(
            f"Uploaded {NUM_FILES} files with {NUM_ROWS_PER_FILE} rows "
            f"each in: {end_time - start_time:.2f} seconds."
        )
