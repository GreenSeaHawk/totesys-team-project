import json
import pytest
import os
import boto3
from moto import mock_aws
from src.transform_to_fact_sales_order import transform_to_fact_sales_order


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


sales_sample_data = [
    {
        "sales_order_id": 1,
        "created_at": 1731947889824,
        "last_updated": 1731947889824,
        "design_id": 101,
        "staff_id": 10,
        "counterparty_id": 1,
        "units_sold": 5000,
        "unit_price": 3.50,
        "currency_id": 1,
        "agreed_delivery_date": "2024-12-01",
        "agreed_payment_date": "2024-12-05",
        "agreed_delivery_location_id": 20,
    },
    {
        "sales_order_id": 2,
        "created_at": 1667485249962,
        "last_updated": 1667485249962,
        "design_id": 102,
        "staff_id": 12,
        "counterparty_id": 2,
        "units_sold": 15000,
        "unit_price": 2.75,
        "currency_id": 2,
        "agreed_delivery_date": "2024-12-10",
        "agreed_payment_date": "2024-12-15",
        "agreed_delivery_location_id": 21,
    },
]

expected_output = [
    {
        "sales_record_id": 1,
        "sales_order_id": 1,
        "created_date": "2024-11-18",
        "created_time": "16:38:09",
        "last_updated_date": "2024-11-18",
        "last_updated_time": "16:38:09",
        "sales_staff_id": 10,
        "counterparty_id": 1,
        "units_sold": 5000,
        "unit_price": 3.5,
        "currency_id": 1,
        "design_id": 101,
        "agreed_payment_date": "2024-12-05",
        "agreed_delivery_date": "2024-12-01",
        "agreed_delivery_location_id": 20,
    },
    {
        "sales_record_id": 2,
        "sales_order_id": 2,
        "created_date": "2022-11-03",
        "created_time": "14:20:49",
        "last_updated_date": "2022-11-03",
        "last_updated_time": "14:20:49",
        "sales_staff_id": 12,
        "counterparty_id": 2,
        "units_sold": 15000,
        "unit_price": 2.75,
        "currency_id": 2,
        "design_id": 102,
        "agreed_payment_date": "2024-12-15",
        "agreed_delivery_date": "2024-12-10",
        "agreed_delivery_location_id": 21,
    },
]

expected_output_2 = [
    {
        "sales_record_id": 3,
        "sales_order_id": 1,
        "created_date": "2024-11-18",
        "created_time": "16:38:09",
        "last_updated_date": "2024-11-18",
        "last_updated_time": "16:38:09",
        "sales_staff_id": 10,
        "counterparty_id": 1,
        "units_sold": 5000,
        "unit_price": 3.5,
        "currency_id": 1,
        "design_id": 101,
        "agreed_payment_date": "2024-12-05",
        "agreed_delivery_date": "2024-12-01",
        "agreed_delivery_location_id": 20,
    },
    {
        "sales_record_id": 4,
        "sales_order_id": 2,
        "created_date": "2022-11-03",
        "created_time": "14:20:49",
        "last_updated_date": "2022-11-03",
        "last_updated_time": "14:20:49",
        "sales_staff_id": 12,
        "counterparty_id": 2,
        "units_sold": 15000,
        "unit_price": 2.75,
        "currency_id": 2,
        "design_id": 102,
        "agreed_payment_date": "2024-12-15",
        "agreed_delivery_date": "2024-12-10",
        "agreed_delivery_location_id": 21,
    },
]


def test_count_starts_at_1_if_no_object_in_s3(create_data_bucket):
    output = transform_to_fact_sales_order(sales_sample_data, "data_bucket")
    expected_json_output = json.dumps(expected_output, separators=(",", ":"))

    assert output == expected_json_output


def test_returns_error_if_sales_order_data_is_empty(create_data_bucket):
    with pytest.raises(Exception, match="Error, sales_order_data is empty"):
        transform_to_fact_sales_order([])


def test_count_saves_to_s3_bucket(s3, create_data_bucket):
    transform_to_fact_sales_order(sales_sample_data, "data_bucket")
    response = s3.get_object(
        Bucket="data_bucket", Key="fact-sales-order-highest-id.txt"
    )
    count = int(response["Body"].read().decode("utf-8"))

    assert count == 3


def test_count_is_both_saved_and_extracted_from_s3_bucket(
    s3, create_data_bucket
):
    transform_to_fact_sales_order(sales_sample_data, "data_bucket")
    output = transform_to_fact_sales_order(sales_sample_data, "data_bucket")
    expected_json_output = json.dumps(expected_output_2, separators=(",", ":"))

    assert output == expected_json_output
