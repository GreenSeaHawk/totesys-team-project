from src.get_data_from_files import get_data_from_files
from moto import mock_aws
import boto3
import os
import pytest
import json
import time
import random


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


@pytest.fixture(scope="function")
def mocked_aws(aws_credentials):
    """
    Mock all AWS interactions
    Requires you to create your own boto3 clients
    """
    with mock_aws():
        yield


@pytest.fixture
def create_ingestion_bucket(s3):
    s3.create_bucket(
        Bucket="ingestion_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.fixture
def populated_ingestion_bucket(s3, create_ingestion_bucket):
    file_key_1 = "payment_type/payment_type_20220101000000.json"
    file_content_1 = [
        {
            "payment_type_id": 1,
            "payment_type_name": "card",
            "created_at": "20230101",
            "last_updated": "20230101",
        },
        {
            "payment_type_id": 2,
            "payment_type_name": "card",
            "created_at": "20230101",
            "last_updated": "20230101",
        },
        {
            "payment_type_id": 3,
            "payment_type_name": "card",
            "created_at": "20230101",
            "last_updated": "20230101",
        },
    ]
    json_content_1 = json.dumps(file_content_1)
    file_key_2 = "payment_type/payment_type_20230101000000.json"
    file_content_2 = [
        {
            "payment_type_id": 4,
            "payment_type_name": "card",
            "created_at": "20230101",
            "last_updated": "20230101",
        },
        {
            "payment_type_id": 5,
            "payment_type_name": "card",
            "created_at": "20230101",
            "last_updated": "20230101",
        },
        {
            "payment_type_id": 6,
            "payment_type_name": "card",
            "created_at": "20230101",
            "last_updated": "20230101",
        },
    ]
    json_content_2 = json.dumps(file_content_2)
    file_key_3 = "address/address_19500101000000.json"
    file_key_4 = "address/address_19600101000000.json"
    file_key_5 = "address/address_20800101000000.json"
    file_key_6 = "address/address_20900101000000.json"
    s3.put_object(
        Bucket="ingestion_bucket", Key=file_key_3, Body=json_content_1
    )
    s3.put_object(
        Bucket="ingestion_bucket", Key=file_key_4, Body=json_content_1
    )
    s3.put_object(
        Bucket="ingestion_bucket", Key=file_key_5, Body=json_content_1
    )
    s3.put_object(
        Bucket="ingestion_bucket", Key=file_key_6, Body=json_content_1
    )
    s3.put_object(
        Bucket="ingestion_bucket", Key=file_key_1, Body=json_content_1
    )
    s3.put_object(
        Bucket="ingestion_bucket", Key=file_key_2, Body=json_content_2
    )


def generate_mock_file_data(num_records):
    """Generate mock JSON data with the specified number of records."""
    return [
        {"id": i, "value": random.randint(1, 1000)} for i in range(num_records)
    ]


class TestGetDataFromFiles:
    def test_returns_list(self, populated_ingestion_bucket):
        files = [
            "payment_type/payment_type_20220101000000.json",
            "payment_type/payment_type_20230101000000.json",
        ]
        assert isinstance(get_data_from_files("ingestion_bucket", files), list)

    def test_returns_data_from_specified_files(
        self, populated_ingestion_bucket
    ):
        files = [
            "payment_type/payment_type_20220101000000.json",
            "payment_type/payment_type_20230101000000.json",
        ]
        result = get_data_from_files(
            Bucket="ingestion_bucket", list_of_files=files
        )
        expected = [
            {
                "payment_type_id": 1,
                "payment_type_name": "card",
                "created_at": "20230101",
                "last_updated": "20230101",
            },
            {
                "payment_type_id": 2,
                "payment_type_name": "card",
                "created_at": "20230101",
                "last_updated": "20230101",
            },
            {
                "payment_type_id": 3,
                "payment_type_name": "card",
                "created_at": "20230101",
                "last_updated": "20230101",
            },
            {
                "payment_type_id": 4,
                "payment_type_name": "card",
                "created_at": "20230101",
                "last_updated": "20230101",
            },
            {
                "payment_type_id": 5,
                "payment_type_name": "card",
                "created_at": "20230101",
                "last_updated": "20230101",
            },
            {
                "payment_type_id": 6,
                "payment_type_name": "card",
                "created_at": "20230101",
                "last_updated": "20230101",
            },
        ]
        assert result == expected

    def test_returns_data_if_no_files(self, populated_ingestion_bucket):
        result = get_data_from_files(
            Bucket="ingestion_bucket", list_of_files=[]
        )
        assert result == []

    def test_get_data_from_files_stress(self, s3, create_ingestion_bucket):
        NUM_FILES = 1000
        RECORDS_PER_FILE = 100

        list_of_files = []
        for i in range(NUM_FILES):
            file_name = f"file_{i}.json"
            file_data = generate_mock_file_data(RECORDS_PER_FILE)
            s3.put_object(
                Bucket="ingestion_bucket",
                Key=file_name,
                Body=json.dumps(file_data),
            )
            list_of_files.append(file_name)

        start_time = time.time()
        data = get_data_from_files(
            Bucket="ingestion_bucket", list_of_files=list_of_files
        )
        end_time = time.time()

        print(f"Number of files processed: {len(list_of_files)}")
        print(f"Number of records returned: = {len(data)}")
        print(f"Execution time: {end_time - start_time:.2f} seconds")

        assert len(data) == NUM_FILES * RECORDS_PER_FILE
        assert all("id" in record and "value" in record for record in data)
