import boto3
from moto import mock_aws
import pytest
import os
import json
from pprint import pprint

# @pytest.fixture
# def aws_credentials():
#     os.environ["AWS_ACCESS_KEY_ID"] = "testing"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
#     os.environ["AWS_SECURITY_TOKEN"] = "testing"
#     os.environ["AWS_SESSION_TOKEN"] = "testing"
#     os.environ["AWS_DEFAULT_REGION"] = 'eu-west-2'

# #fixture for mock_aws
# @mock_aws
# @pytest.fixture
# def s3_client(aws_credentials):
#     return boto3.client("s3", region_name="eu-west-2")

# @pytest.fixture(scope="function")
# def mocked_aws(aws_credentials):
#     """
#     Mock all AWS interactions
#     Requires you to create your own boto3 clients
#     """
#     with mock_aws():
#         yield

# #fixture for ingestion s3 bucket
# @pytest.fixture
# def s3_bucket_ingestion(s3_client):
#     bucket_name = "ingestion-bucket"
#     s3_client.create_bucket(
#         Bucket=bucket_name,
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
#     )
#     # return bucket_name

# #fixture for ingestion bucket with content
# @pytest.fixture
# def s3_bucket_ingestion_content(s3_client, s3_bucket_ingestion):
#     file_key_1 = "payment_type/payment_type_20220101000000.json"
#     file_content_1 = [{'payment_type_id': 1, 'payment_type_name': 'card', 'created_at': '20230101', 'last_updated': '20230101'}, {'payment_type_id': 2, 'payment_type_name': 'card', 'created_at': '20230101', 'last_updated': '20230101'}, {'payment_type_id': 3, 'payment_type_name': 'card', 'created_at': '20230101', 'last_updated': '20230101'}]
#     json_content_1 = json.dumps(file_content_1)
#     file_key_2 = "payment_type/payment_type_20230101000000.json"
#     file_content_2 = [{'payment_type_id': 4, 'payment_type_name': 'card', 'created_at': '20230101', 'last_updated': '20230101'}, {'payment_type_id': 5, 'payment_type_name': 'card', 'created_at': '20230101', 'last_updated': '20230101'}, {'payment_type_id': 6, 'payment_type_name': 'card', 'created_at': '20230101', 'last_updated': '20230101'}]
#     json_content_2 = json.dumps(file_content_2)
#     s3_client.put_object(Bucket=s3_bucket_ingestion, Key=file_key_1, Body=json_content_1)
#     s3_client.put_object(Bucket=s3_bucket_ingestion, Key=file_key_2, Body=json_content_2)
#     return s3_bucket_ingestion, file_key_1, json_content_1

# #fixture for empty transform bucket
# @pytest.fixture
# def s3_bucket_transform_empty(s3_client):
#     bucket_name = "transform-bucket"
#     s3_client.create_bucket(
#         Bucket=bucket_name,
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
#     )
#     return bucket_name

# #fixture for transform bucket with content
# @pytest.fixture
# def s3_bucket_transform(s3_client, s3_bucket_transform_empty):
#     file_key = "last_run.json"
#     file_content = '20220301143000'
#     s3_client.put_object(Bucket=s3_bucket_transform_empty, Key=file_key, Body=file_content)
#     return s3_bucket_transform_empty, file_key, file_content

# @mock_aws
# def test_objects_in_mock_buckets(s3_client, s3_bucket_transform):
#     response = s3_client.get_object(Bucket='ingestion-bucket', Key="payment_type/payment_type_20220101000000.json")
#     pprint(response)
#     assert False

# def test(s3_bucket_ingestion, mocked_aws):
#     s3_client = boto3.client("s3", region_name="eu-west-2")
#     s3_client.create_bucket(
#         Bucket = 'test',
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
#     )
#     assert False


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
    s3.put_object(
        Bucket='ingestion_bucket', Key=file_key_1, Body=json_content_1
    )
    s3.put_object(
        Bucket='ingestion_bucket', Key=file_key_2, Body=json_content_2
    )

@pytest.fixture
def populated_transform_bucket(s3, create_transform_bucket):
    file_key = "last_run.json"
    file_content = '20220301143000'
    s3.put_object(Bucket='transform_bucket', Key=file_key, Body=file_content)

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


    def test_generic_aws_fixture(self, mocked_aws):
        s3_client = boto3.client("s3")
        s3_client.create_bucket(
            Bucket="somebucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

    def test_populate_ingestion_bucket(self, s3, populated_ingestion_bucket):
        response = s3.list_objects_v2(Bucket='ingestion_bucket')
        assert len(response['Contents']) == 2

    def test_populate_transform_bucket(self, s3, populated_transform_bucket):
        response = s3.list_objects_v2(Bucket='transform_bucket')
        assert len(response['Contents']) == 1
