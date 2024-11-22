import boto3
from moto import mock_aws
import pytest
import os
import json
import random
import string
import time
from src.list_all_filenames import list_all_filenames_in_s3


def generate_random_filename():
    """Generate a random file with a timestamp"""
    timestamp = random.randint(1500000001, 2000000000)
    random_str = "".join(
        random.choices(string.ascii_letters + string.digits, k=10)
    )
    return f"{random_str}_{timestamp}.json"


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
def create_transform_bucket(s3):
    s3.create_bucket(
        Bucket="transform_bucket",
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


@pytest.fixture
def transform_bucket_2022(s3, create_transform_bucket):
    file_key = "last_run.json"
    file_content = "20210101000000"
    s3.put_object(Bucket="transform_bucket", Key=file_key, Body=file_content)


@pytest.fixture
def transform_bucket_2022_dec(s3, create_transform_bucket):
    file_key = "last_run.json"
    file_content = "20221201143000"
    s3.put_object(Bucket="transform_bucket", Key=file_key, Body=file_content)


@pytest.fixture
def transform_bucket_2025(s3, create_transform_bucket):
    file_key = "last_run.json"
    file_content = "20251201143000"
    s3.put_object(Bucket="transform_bucket", Key=file_key, Body=file_content)


class TestMockFixtures:
    def test_s3_bucket_creation(self, s3):
        s3.create_bucket(
            Bucket="somebucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        result = s3.list_buckets()
        assert len(result["Buckets"]) == 1

    def test_s3_bucket_creation_through_fixtures(
        self, create_ingestion_bucket, create_transform_bucket
    ):
        result = boto3.client("s3").list_buckets()
        assert len(result["Buckets"]) == 2

    def test_populate_ingestion_bucket(self, s3, populated_ingestion_bucket):
        response = s3.list_objects_v2(Bucket="ingestion_bucket")
        assert len(response["Contents"]) == 6

    def test_generic_aws_fixture(self, mocked_aws):
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="somebucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

    def test_populate_transform_bucket(self, s3, transform_bucket_2022):
        response = s3.list_objects_v2(Bucket="transform_bucket")
        assert len(response["Contents"]) == 1


class TestListAllFileNames:
    def test_returns_a_list(
        self, populated_ingestion_bucket, transform_bucket_2022
    ):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket", prefix="payment_type"
        )
        assert isinstance(result, list)

    def test_list_all_filnames_returns_all_filenames(
        self, populated_ingestion_bucket, transform_bucket_2022
    ):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket", prefix="payment_type"
        )
        expected = [
            "payment_type/payment_type_20220101000000.json",
            "payment_type/payment_type_20230101000000.json",
        ]
        assert f"Expected {expected} but got {result}"
        assert result == expected

    def test_list_all_filnames_returns_all_filenames_after_2022_december(
        self, populated_ingestion_bucket, transform_bucket_2022_dec
    ):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket", prefix="payment_type"
        )
        expected = ["payment_type/payment_type_20230101000000.json"]
        assert result == expected

    def test_list_all_filnames_returns_all_filenames_after_2025(
        self, populated_ingestion_bucket, transform_bucket_2025
    ):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket", prefix="payment_type"
        )
        expected = []  # NO files in 'payment_type' after 2025.
        assert result == expected

    def test_list_all_filnames_returns_all_filenames_after_2025_address(
        self, populated_ingestion_bucket, transform_bucket_2025
    ):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket", prefix="address"
        )
        expected = [
            "address/address_20800101000000.json",
            "address/address_20900101000000.json",
        ]
        assert result == expected

    def test_list_all_file_names_no_matching_files(
        self, populated_ingestion_bucket, transform_bucket_2025
    ):

        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket", prefix="payment_type"
        )
        assert result == []  # No files newer than the last_run.json timestamp

    def test_wrong_prefix(
        self, populated_ingestion_bucket, transform_bucket_2025
    ):
        with pytest.raises(
            NameError, match="No files found in s3://ingestion_bucket/hi"
        ):
            list_all_filenames_in_s3(Bucket="ingestion_bucket", prefix="hi")

    def test_list_all_filenames_in_s3_stress(
        self, s3, create_ingestion_bucket, create_transform_bucket
    ):
        BUCKET_NAME = "ingestion_bucket"
        TRANSFORM_BUCKET_NAME = "transform_bucket"
        LAST_RUN_KEY = "last_run.json"

        last_run_timestamp = 1400000000  # as an example
        s3.put_object(  # example timestamp
            Bucket=TRANSFORM_BUCKET_NAME,
            Key=LAST_RUN_KEY,
            Body=str(last_run_timestamp),
        )

        number_of_files = 30
        for _ in range(number_of_files):
            filename = generate_random_filename()
            s3.put_object(
                Bucket=BUCKET_NAME, Key=filename, Body="Test content"
            )

        # measure execution time of the function
        start_time = time.time()
        file_names = list_all_filenames_in_s3(Bucket=BUCKET_NAME)
        end_time = time.time()

        print(f"Number of files returned: {len(file_names)}")
        print(f"Execution time: {end_time - start_time:.2f} seconds.")

        assert len(file_names) > 0