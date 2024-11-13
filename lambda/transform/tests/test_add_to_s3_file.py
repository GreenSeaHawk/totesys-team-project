from src.add_to_s3_file import add_to_s3_file
import pytest, boto3, os, json
from pprint import pprint
from moto import mock_aws

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

    def test_file_added_have_correct_names(self, s3, create_transform_bucket):
        add_to_s3_file("transform_bucket", [{'key':1}], 'address')
        response = s3.list_objects_v2(Bucket='transform_bucket', Prefix='address')
        pprint(response['Contents'])
        assert response['Contents']['Key']