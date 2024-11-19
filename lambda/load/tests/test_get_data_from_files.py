import pytest
import os
import io
import boto3
import pandas as pd
from moto import mock_aws
from src.get_data_from_files import get_data_from_files


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
def create_transform_bucket(s3):
    s3.create_bucket(
        Bucket="transform_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

@pytest.fixture
def populated_transform_bucket(s3, create_transform_bucket):
    file_key_1 = "dim_payment/2022/01/dim_payment_20220101000000.parquet"
    file_content_1 = [
  {
    "payment_type_id": 4,
    "payment_type_name": "SALES_RECEIPT"
  },
  {
    "payment_type_id": 5,
    "payment_type_name": "PURCHASE_PAYMENT"
  },
  {
    "payment_type_id": 6,
    "payment_type_name": "REFUND"
  }
]
    df = pd.DataFrame(file_content_1)
    # Write the DataFrame to an in-memory buffer
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow")
    buffer.seek(0)  # Reset buffer position to the beginning

    s3.put_object(Bucket="transform_bucket", Key=file_key_1, Body=buffer.getvalue())

    file_key_2 = "dim_payment/2023/01/dim_payment_20230101000000.parquet"
    file_content_2 = [
  {
    "payment_type_id": 1,
    "payment_type_name": "SALES_RECEIPT"
  },
  {
    "payment_type_id": 2,
    "payment_type_name": "PURCHASE_PAYMENT"
  },
  {
    "payment_type_id": 3,
    "payment_type_name": "REFUND"
  }
]
    df2 = pd.DataFrame(file_content_2)
    # Write the DataFrame to an in-memory buffer
    buffer = io.BytesIO()
    df2.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)  # Reset buffer position to the beginning

    s3.put_object(Bucket="transform_bucket", Key=file_key_2, Body=buffer.getvalue())

class TestMockModules:
    def test_populate_transform_bucket(self, s3, populated_transform_bucket):
        response = s3.list_objects_v2(Bucket="transform_bucket")
        assert len(response["Contents"]) == 2



class TestGetDataFromFiles:
    def test_returns_dataframe(self, populated_transform_bucket):
        file = ["dim_payment/2023/01/dim_payment_20230101000000.parquet"]
        response = get_data_from_files("transform_bucket", file)

        assert isinstance(response, pd.core.frame.DataFrame)

    def test_content_of_1_dataframe(self, populated_transform_bucket):
        file = ["dim_payment/2023/01/dim_payment_20230101000000.parquet"]
        response = get_data_from_files("transform_bucket", file)
        file_content_2 = [
  {
    "payment_type_id": 1,
    "payment_type_name": "SALES_RECEIPT"
  },
  {
    "payment_type_id": 2,
    "payment_type_name": "PURCHASE_PAYMENT"
  },
  {
    "payment_type_id": 3,
    "payment_type_name": "REFUND"
  }
]
        expected_output = pd.DataFrame(file_content_2)

        assert response.equals(expected_output)
    
    def test_content_of_2_dataframes(self, populated_transform_bucket):
        files = ["dim_payment/2023/01/dim_payment_20230101000000.parquet",
                "dim_payment/2022/01/dim_payment_20220101000000.parquet"]
        response = get_data_from_files("transform_bucket", files)

        file_content_2 = [
  {
    "payment_type_id": 1,
    "payment_type_name": "SALES_RECEIPT"
  },
  {
    "payment_type_id": 2,
    "payment_type_name": "PURCHASE_PAYMENT"
  },
  {
    "payment_type_id": 3,
    "payment_type_name": "REFUND"
  },
    {
    "payment_type_id": 4,
    "payment_type_name": "SALES_RECEIPT"
  },
  {
    "payment_type_id": 5,
    "payment_type_name": "PURCHASE_PAYMENT"
  },
  {
    "payment_type_id": 6,
    "payment_type_name": "REFUND"
  }
]
        expected_output = pd.DataFrame(file_content_2)

        assert response.equals(expected_output)

    def test_returns_data_if_no_files(self, populated_transform_bucket):
        result = get_data_from_files("transform_bucket", [])
        assert result.equals(pd.DataFrame())


