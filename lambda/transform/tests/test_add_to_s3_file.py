from src.add_to_s3_file import add_to_s3_file, list_all_objects_in_bucket
import pytest, boto3, os, json
import time
import concurrent.futures
from pprint import pprint
from moto import mock_aws
from freezegun import freeze_time
from datetime import datetime
from unittest.mock import MagicMock, patch
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
        add_to_s3_file("transform_bucket", [{'key':1}], 'address')
        response = s3.list_objects_v2(Bucket='transform_bucket', Prefix='address')
        assert len(response['Contents']) == 1

    @freeze_time("2023-01-01")
    def test_file_added_have_correct_names(self, s3, create_transform_bucket):
        add_to_s3_file("transform_bucket", [{'key':1}], 'address')
        response = s3.list_objects_v2(Bucket='transform_bucket', Prefix='address')
        assert response['Contents'][0]['Key'] ==  'address/2023/address_20230101000000000000.json'

    def test_multiple_files_added(self, s3, create_transform_bucket):
        add_to_s3_file("transform_bucket", [{'key':1}], 'address')
        add_to_s3_file("transform_bucket", [{'key':2}], 'address')
        response = s3.list_objects_v2(Bucket='transform_bucket', Prefix='address')
        assert len(response['Contents']) == 2

    @freeze_time("2023-01-01")
    def test_success_message(self, create_transform_bucket, capsys):
        add_to_s3_file("transform_bucket", [{'key':1}], 'address')
        captured = capsys.readouterr()
        assert captured.out.strip() == 'Object address/2023/address_20230101000000000000.json uploaded successfully to s3://transform_bucket.'

# we are uploading 100000 files to s3. How would it behave? Concurrent uploads?
    def test_add_to_s3_stress(self, s3, create_transform_bucket):
        NUM_FILES = 8
        RECORDS_PER_FILE = 100 
        TABLE_NAME= "test_table"

        # Generate mock data for concurrent uploads
        def upload_file(index):
            mock_data = generate_mock_file_data(RECORDS_PER_FILE)
            add_to_s3_file(bucket="transform_bucket", data =mock_data, table=TABLE_NAME)

        # For SEQUENTIAL, uncomment the following code.
        # for i in range(NUM_FILES):
        #     mock_data = generate_mock_file_data(RECORDS_PER_FILE)
        #     add_to_s3_file(bucket="transform_bucket", data =mock_data, table=TABLE_NAME)

        # for concurrent uploads
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            my_futures = [executor.submit(upload_file, i) for i in range(NUM_FILES)]
            concurrent.futures.wait(my_futures)    

        # validate the results using pagination
        all_objects = list_all_objects_in_bucket(bucket_name="transform_bucket")
        uploaded_files = len(all_objects)

        print(f"Number of files uploaded: {uploaded_files}")
        assert uploaded_files == NUM_FILES