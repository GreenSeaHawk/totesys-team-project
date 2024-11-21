import json
import pytest
import os
import boto3
from moto import mock_aws
from src.transform_to_fact_purchase_order import (
    transform_to_fact_purchase_order,
)


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


purchase_order_data_sample = [
    {
        "purchase_order_id": 1,
        "created_at": 1731947889824,
        "last_updated": 1731947889824,
        "staff_id": 101,
        "counterparty_id": 2001,
        "item_code": "ABC123",
        "item_quantity": 150,
        "item_unit_price": 75.50,
        "currency_id": 1,
        "agreed_delivery_date": "2024-11-25",
        "agreed_payment_date": "2024-11-20",
        "agreed_delivery_location_id": 5001,
    },
    {
        "purchase_order_id": 2,
        "created_at": 1667485249962,
        "last_updated": 1667485249962,
        "staff_id": 102,
        "counterparty_id": 2002,
        "item_code": "XYZ789",
        "item_quantity": 500,
        "item_unit_price": 30.00,
        "currency_id": 2,
        "agreed_delivery_date": "2024-12-01",
        "agreed_payment_date": "2024-11-22",
        "agreed_delivery_location_id": 5002,
    },
    {
        "purchase_order_id": 3,
        "created_at": 1731947889824,
        "last_updated": 1731947889824,
        "staff_id": 103,
        "counterparty_id": 2003,
        "item_code": "LMN456",
        "item_quantity": 1000,
        "item_unit_price": 45.25,
        "currency_id": 3,
        "agreed_delivery_date": "2024-11-30",
        "agreed_payment_date": "2024-11-25",
        "agreed_delivery_location_id": 5003,
    },
]

expected_output = [
    {
        "purchase_record_id": 1,
        "purchase_order_id": 1,
        "created_date": "2024-11-18",
        "created_time": "16:38:09",
        "last_updated_date": "2024-11-18",
        "last_updated_time": "16:38:09",
        "staff_id": 101,
        "counterparty_id": 2001,
        "item_code": "ABC123",
        "item_quantity": 150,
        "item_unit_price": 75.5,
        "currency_id": 1,
        "agreed_delivery_date": "2024-11-25",
        "agreed_payment_date": "2024-11-20",
        "agreed_delivery_location_id": 5001,
    },
    {
        "purchase_record_id": 2,
        "purchase_order_id": 2,
        "created_date": "2022-11-03",
        "created_time": "14:20:49",
        "last_updated_date": "2022-11-03",
        "last_updated_time": "14:20:49",
        "staff_id": 102,
        "counterparty_id": 2002,
        "item_code": "XYZ789",
        "item_quantity": 500,
        "item_unit_price": 30.0,
        "currency_id": 2,
        "agreed_delivery_date": "2024-12-01",
        "agreed_payment_date": "2024-11-22",
        "agreed_delivery_location_id": 5002,
    },
    {
        "purchase_record_id": 3,
        "purchase_order_id": 3,
        "created_date": "2024-11-18",
        "created_time": "16:38:09",
        "last_updated_date": "2024-11-18",
        "last_updated_time": "16:38:09",
        "staff_id": 103,
        "counterparty_id": 2003,
        "item_code": "LMN456",
        "item_quantity": 1000,
        "item_unit_price": 45.25,
        "currency_id": 3,
        "agreed_delivery_date": "2024-11-30",
        "agreed_payment_date": "2024-11-25",
        "agreed_delivery_location_id": 5003,
    },
]

expected_output_2 = [
    {
        "purchase_record_id": 4,
        "purchase_order_id": 1,
        "created_date": "2024-11-18",
        "created_time": "16:38:09",
        "last_updated_date": "2024-11-18",
        "last_updated_time": "16:38:09",
        "staff_id": 101,
        "counterparty_id": 2001,
        "item_code": "ABC123",
        "item_quantity": 150,
        "item_unit_price": 75.5,
        "currency_id": 1,
        "agreed_delivery_date": "2024-11-25",
        "agreed_payment_date": "2024-11-20",
        "agreed_delivery_location_id": 5001,
    },
    {
        "purchase_record_id": 5,
        "purchase_order_id": 2,
        "created_date": "2022-11-03",
        "created_time": "14:20:49",
        "last_updated_date": "2022-11-03",
        "last_updated_time": "14:20:49",
        "staff_id": 102,
        "counterparty_id": 2002,
        "item_code": "XYZ789",
        "item_quantity": 500,
        "item_unit_price": 30.0,
        "currency_id": 2,
        "agreed_delivery_date": "2024-12-01",
        "agreed_payment_date": "2024-11-22",
        "agreed_delivery_location_id": 5002,
    },
    {
        "purchase_record_id": 6,
        "purchase_order_id": 3,
        "created_date": "2024-11-18",
        "created_time": "16:38:09",
        "last_updated_date": "2024-11-18",
        "last_updated_time": "16:38:09",
        "staff_id": 103,
        "counterparty_id": 2003,
        "item_code": "LMN456",
        "item_quantity": 1000,
        "item_unit_price": 45.25,
        "currency_id": 3,
        "agreed_delivery_date": "2024-11-30",
        "agreed_payment_date": "2024-11-25",
        "agreed_delivery_location_id": 5003,
    },
]


def test_count_starts_at_1_if_no_object_in_s3(create_data_bucket):
    output = transform_to_fact_purchase_order(
        purchase_order_data_sample, "data_bucket"
    )
    expected_json_output = json.dumps(expected_output, separators=(",", ":"))
    print(output)
    assert output == expected_json_output


def test_returns_error_if_purchase_order_data_is_empty():
    with pytest.raises(Exception, match="Error, purchase_order_data is empty"):
        transform_to_fact_purchase_order([])


def test_count_starts_at_1_if_no_object_in_s3(create_data_bucket):
    output = transform_to_fact_purchase_order(
        purchase_order_data_sample, "data_bucket"
    )
    expected_json_output = json.dumps(expected_output, separators=(",", ":"))

    assert output == expected_json_output


def test_count_saves_to_s3_bucket(s3, create_data_bucket):
    transform_to_fact_purchase_order(purchase_order_data_sample, "data_bucket")
    response = s3.get_object(
        Bucket="data_bucket", Key="fact-purchase-order-highest-id.txt"
    )
    count = int(response["Body"].read().decode("utf-8"))

    assert count == 4


def test_count_is_both_saved_and_extracted_from_s3_bucket(
    s3, create_data_bucket
):
    transform_to_fact_purchase_order(purchase_order_data_sample, "data_bucket")
    output = transform_to_fact_purchase_order(
        purchase_order_data_sample, "data_bucket"
    )
    expected_json_output = json.dumps(expected_output_2, separators=(",", ":"))

    assert output == expected_json_output
