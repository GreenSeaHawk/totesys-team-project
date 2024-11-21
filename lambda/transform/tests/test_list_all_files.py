import boto3
import pytest
import os
import json
import random
import string
import time
from moto import mock_aws
from datetime import datetime
from src.list_all_files import (
    list_all_filenames_in_s3,
    get_last_ran,
    update_last_ran_s3,
)


def generate_random_filename():
    """Generate a random file with a timestamp"""
    timestamp = random.randint(150000000000000001, 200000000000000000)
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
        Bucket="totesys-transformed-data-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.fixture
def populated_ingestion_bucket(s3, create_ingestion_bucket):
    file_key_1 = "payment_type/2022/01/payment_type_202201010000000000.json"
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
    file_key_2 = "payment_type/2023/01/payment_type_202301010000000000.json"
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
    file_key_3 = "address/1950/01/address_195001010000000000.json"
    file_key_4 = "address/1960/01/address_196001010000000000.json"
    file_key_5 = "address/2080/01/address_208001010000000000.json"
    file_key_6 = "address/2090/01/address_209001010000000000.json"
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
    file_content = "202101010000000000"
    s3.put_object(
        Bucket="totesys-transformed-data-bucket",
        Key=file_key,
        Body=file_content,
    )


@pytest.fixture
def transform_bucket_2022_dec(s3, create_transform_bucket):
    file_key = "last_run.json"
    file_content = "202212011430000000"
    s3.put_object(
        Bucket="totesys-transformed-data-bucket",
        Key=file_key,
        Body=file_content,
    )


@pytest.fixture
def transform_bucket_2025(s3, create_transform_bucket):
    file_key = "last_run.json"
    file_content = "202512011430000000"
    s3.put_object(
        Bucket="totesys-transformed-data-bucket",
        Key=file_key,
        Body=file_content,
    )


@pytest.fixture
def setup_s3_bucket():
    """Mock the S3 bucket for testing with moto."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket="totesys-transformed-data-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


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
        response = s3.list_objects_v2(Bucket="totesys-transformed-data-bucket")
        assert len(response["Contents"]) == 1


class TestListAllFileNames:
    def test_returns_a_list(self, populated_ingestion_bucket):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket",
            last_run_timestamp=20210101000000,
            prefix="payment_type",
        )
        assert isinstance(result, list)

    def test_list_all_filnames_returns_all_filenames(
        self, populated_ingestion_bucket
    ):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket",
            last_run_timestamp=202101010000000000,
            prefix="payment_type",
        )
        expected = [
            "payment_type/2022/01/payment_type_202201010000000000.json",
            "payment_type/2023/01/payment_type_202301010000000000.json",
        ]
        assert f"Expected {expected} but got {result}"
        assert result == expected

    def test_list_all_filnames_returns_all_filenames_after_2022_december(
        self, populated_ingestion_bucket
    ):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket",
            last_run_timestamp=202212011430000000,
            prefix="payment_type",
        )
        expected = [
            "payment_type/2023/01/payment_type_202301010000000000.json"
        ]
        assert result == expected

    def test_list_all_filnames_returns_all_filenames_after_2025(
        self, populated_ingestion_bucket
    ):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket",
            last_run_timestamp=202512011430000000,
            prefix="payment_type",
        )
        expected = []  # NO files in 'payment_type' after 2025.
        assert result == expected

    def test_list_all_filnames_returns_all_filenames_after_2025_address(
        self, populated_ingestion_bucket
    ):
        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket",
            last_run_timestamp=202512011430000000,
            prefix="address",
        )
        expected = [
            "address/2080/01/address_208001010000000000.json",
            "address/2090/01/address_209001010000000000.json",
        ]
        assert result == expected

    def test_list_all_file_names_no_matching_files(
        self, populated_ingestion_bucket
    ):

        result = list_all_filenames_in_s3(
            Bucket="ingestion_bucket",
            last_run_timestamp=202512011430000000,
            prefix="payment_type",
        )
        assert result == []  # No files newer than the last_run.json timestamp

    def test_wrong_prefix(self, populated_ingestion_bucket):
        with pytest.raises(
            NameError, match="No files found in s3://ingestion_bucket/hi"
        ):
            list_all_filenames_in_s3(
                Bucket="ingestion_bucket",
                last_run_timestamp=202512011430000000,
                prefix="hi",
            )

    def test_list_all_filenames_in_s3_stress(
        self, s3, create_ingestion_bucket, create_transform_bucket
    ):
        BUCKET_NAME = "ingestion_bucket"
        TRANSFORM_BUCKET_NAME = "totesys-transformed-data-bucket"
        LAST_RUN_KEY = "last_run.json"

        last_run_timestamp = 140000000000000000  # as an example
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
        file_names = list_all_filenames_in_s3(
            Bucket=BUCKET_NAME, last_run_timestamp=150000000000000001
        )
        end_time = time.time()

        print(f"Number of files returned: {len(file_names)}")
        print(f"Execution time: {end_time - start_time:.2f} seconds.")

        assert len(file_names) > 0


class TestTimeKeyFunctions:
    def test_get_last_ran_when_no_file(self, create_transform_bucket):
        result = get_last_ran("totesys-transformed-data-bucket")

        assert result == datetime(1900, 1, 1)

    def test_update_last_ran_puts_current_time(
        self, s3, create_transform_bucket
    ):
        update_last_ran_s3("totesys-transformed-data-bucket")
        response = s3.get_object(
            Bucket="totesys-transformed-data-bucket", Key="last_ran.json"
        )
        stored_time = response["Body"].read().decode("utf-8")
        current_time = datetime.fromisoformat(stored_time)
        assert (datetime.now() - current_time).total_seconds() < 3

    def test_get_last_ran_gets_updated_timestamp(
        self, s3, create_transform_bucket
    ):
        update_last_ran_s3("totesys-transformed-data-bucket")
        response = get_last_ran("totesys-transformed-data-bucket")

        assert (datetime.now() - response).total_seconds() < 3

