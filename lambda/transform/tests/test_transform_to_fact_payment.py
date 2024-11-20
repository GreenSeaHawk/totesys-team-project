import json
import pytest
import boto3
import os
from src.transform_to_fact_payment import transform_to_fact_payment
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
def create_data_bucket(s3):
    s3.create_bucket(
        Bucket="data_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

payment_type_sample_data = [
  {
    "payment_id": 1,
    "created_at": "2024-11-10T08:00:00Z",
    "last_updated": "2024-11-10T08:00:00Z",
    "transaction_id": 101,
    "counterparty_id": 1,
    "payment_amount": 5000.75,
    "currency_id": 1,
    "payment_type_id": 1,
    "paid": True,
    "payment_date": "2024-11-10",
    "company_ac_number": 12345678,
    "counterparty_ac_number": 87654321
  },
  {
    "payment_id": 2,
    "created_at": "2024-11-15T10:30:00Z",
    "last_updated": "2024-11-15T10:30:00Z",
    "transaction_id": 102,
    "counterparty_id": 2,
    "payment_amount": 25000.00,
    "currency_id": 2,
    "payment_type_id": 2,
    "paid": False,
    "payment_date": "2024-12-01",
    "company_ac_number": 23456789,
    "counterparty_ac_number": 98765432
  }
]

expected_output = [
  {
    "payment_record_id": 1,
    "payment_id": 1,
    "created_date": "2024-11-10",
    "created_time": "08:00:00",
    "last_updated_date": "2024-11-10",
    "last_updated_time": "08:00:00",
    "transaction_id": 101,
    "counterparty_id": 1,
    "payment_amount": 5000.75,
    "currency_id": 1,
    "payment_type_id": 1,
    "paid": True,
    "payment_date": "2024-11-10"
  },
  {
    "payment_record_id": 2,
    "payment_id": 2,
    "created_date": "2024-11-15",
    "created_time": "10:30:00",
    "last_updated_date": "2024-11-15",
    "last_updated_time": "10:30:00",
    "transaction_id": 102,
    "counterparty_id": 2,
    "payment_amount": 25000.0,
    "currency_id": 2,
    "payment_type_id": 2,
    "paid": False,
    "payment_date": "2024-12-01"
  }
]

expected_output_2 = [
  {
    "payment_record_id": 3,
    "payment_id": 1,
    "created_date": "2024-11-10",
    "created_time": "08:00:00",
    "last_updated_date": "2024-11-10",
    "last_updated_time": "08:00:00",
    "transaction_id": 101,
    "counterparty_id": 1,
    "payment_amount": 5000.75,
    "currency_id": 1,
    "payment_type_id": 1,
    "paid": True,
    "payment_date": "2024-11-10"
  },
  {
    "payment_record_id": 4,
    "payment_id": 2,
    "created_date": "2024-11-15",
    "created_time": "10:30:00",
    "last_updated_date": "2024-11-15",
    "last_updated_time": "10:30:00",
    "transaction_id": 102,
    "counterparty_id": 2,
    "payment_amount": 25000.0,
    "currency_id": 2,
    "payment_type_id": 2,
    "paid": False,
    "payment_date": "2024-12-01"
  }
]

def test_count_starts_at_1_if_no_object_in_s3(create_data_bucket):
  output = transform_to_fact_payment(payment_type_sample_data, "data_bucket")
  expected_json_output = json.dumps(expected_output, separators=(',',':'))

  assert output == expected_json_output

def test_returns_error_if_payment_data_is_empty(create_data_bucket):
  with pytest.raises(Exception, match='Error, payment_data is empty'):
    transform_to_fact_payment([])


def test_count_starts_at_1_if_no_object_in_s3(create_data_bucket):
  output = transform_to_fact_payment(payment_type_sample_data, "data_bucket")
  expected_json_output = json.dumps(expected_output, separators=(',',':'))

  assert output == expected_json_output

def test_count_saves_to_s3_bucket(s3, create_data_bucket):
  transform_to_fact_payment(payment_type_sample_data, "data_bucket")
  response = s3.get_object(
    Bucket="data_bucket", 
    Key='fact-payment-highest-id.txt')
  count = int(response['Body'].read().decode("utf-8"))
  
  assert count == 3


def test_count_is_both_saved_and_extracted_from_s3_bucket(s3, create_data_bucket):
  transform_to_fact_payment(payment_type_sample_data, "data_bucket")
  output = transform_to_fact_payment(payment_type_sample_data, "data_bucket")
  expected_json_output = json.dumps(expected_output_2, separators=(',',':'))
  
  assert output == expected_json_output



