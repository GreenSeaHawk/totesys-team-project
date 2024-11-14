from src.add_to_s3_file import add_to_s3_file
import pytest, boto3, os, json
from pprint import pprint
from moto import mock_aws
from freezegun import freeze_time
from datetime import datetime
import time
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

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
        print(datetime.now())
        assert response['Contents'][0]['Key'] ==  'address/address_20230101000000000000.json'

    def test_multiple_files_added(self, s3, create_transform_bucket):
        add_to_s3_file("transform_bucket", [{'key':1}], 'address')
        add_to_s3_file("transform_bucket", [{'key':2}], 'address')
        response = s3.list_objects_v2(Bucket='transform_bucket', Prefix='address')
        assert len(response['Contents']) == 2

    @freeze_time("2023-01-01")
    def test_success_message(self, create_transform_bucket, capsys):
        add_to_s3_file("transform_bucket", [{'key':1}], 'address')
        captured = capsys.readouterr()
        assert captured.out.strip() == 'Object address/address_20230101000000000000.json uploaded successfully to s3://transform_bucket.'

    # @patch('src.add_to_s3_file.boto3.client')
    # def test_error_message(self, mock_s3_client):
    #     mock_s3 = MagicMock()
    #     mock_s3.put_object.side_effect = ClientError(error_response={'Error':{'Code':500, 'Message':'InternalServiceError'}}, operation_name='PutObject')
    #     mock_s3_client.return_value = mock_s3
    #     with pytest.raises(Exception, match='Failed to upload data to'):
    #         add_to_s3_file("transform_bucket", [{'key':1}], 'address')
