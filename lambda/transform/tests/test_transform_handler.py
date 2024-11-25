from unittest.mock import patch
from src.transform_handler import transform_handler
from datetime import datetime


# Tables
TABLES = [
    "counterparty",
    "currency",
    "department",
    "design",
    "staff",
    "sales_order",
    "address",
    "payment",
    "purchase_order",
    "payment_type",
    "transaction",
]

transformed_tables = {
    "counterparty": "dim_counterparty",
    "currency": "dim_currency",
    "date": "dim_date",
    "design": "dim_design",
    "location": "dim_location",
    "payment_type": "dim_payment_type",
    "staff": "dim_staff",
    "transaction": "dim_transaction",
    "payment": "fact_payment",
    "purchase_order": "fact_purchase_order",
    "sales_order": "fact_sales_order",
}

source_bucket = "totesys-data-bucket-cimmeria"
transform_bucket = "totesys-transformed-data-bucket"
first_timestamp = datetime(1900, 1, 1)


@patch("src.transform_handler.get_data_from_files")
@patch("src.transform_handler.list_all_filenames_in_s3")
@patch("src.transform_handler.update_last_ran_s3")
@patch("src.transform_handler.get_last_ran")
@patch("src.transform_handler.transform_to_dim_counterparty")
@patch("src.transform_handler.transform_to_dim_currency")
@patch("src.transform_handler.transform_to_dim_design")
@patch("src.transform_handler.transform_to_dim_location")
@patch("src.transform_handler.transform_to_dim_payment_type")
@patch("src.transform_handler.transform_to_dim_staff")
@patch("src.transform_handler.transform_to_dim_date")
@patch("src.transform_handler.transform_to_dim_transaction")
@patch("src.transform_handler.transform_to_fact_payment")
@patch("src.transform_handler.transform_to_fact_purchase_order")
@patch("src.transform_handler.transform_to_fact_sales_order")
@patch("src.transform_handler.filter_latest_data")
@patch("src.transform_handler.write_transformed_data_to_s3")
def test_transform_handler_calls_utils_when_only_currency_data(
    mock_write_transformed_data_to_s3,
    mock_filter_latest_data,
    mock_transform_to_fact_sales_order,
    mock_transform_to_fact_purchase_order,
    mock_transform_to_fact_payment,
    mock_transform_to_dim_transaction,
    mock_transform_to_dim_date,
    mock_transform_to_dim_staff,
    mock_transform_to_dim_payment_type,
    mock_transform_to_dim_location,
    mock_transform_to_dim_design,
    mock_transform_to_dim_currency,
    mock_transform_to_dim_counterparty,
    mock_get_last_ran,
    mock_update_last_ran_s3,
    mock_list_all_filenames_in_s3,
    mock_get_data_from_files,
):
    mock_get_last_ran.return_value = "test-last-timestamp"

    def side_effect_list_files(Bucket, last_run_timestamp, prefix):
        if (
            prefix == "currency"
            and Bucket == source_bucket
            and last_run_timestamp == "test-last-timestamp"
        ):
            return "currency files"

    mock_list_all_filenames_in_s3.side_effect = side_effect_list_files
    mock_get_data_from_files.return_value = "currency_data"
    mock_transform_to_dim_currency.return_value = "transformed_currency"

    transform_handler("event", "context")

    # Assert
    mock_get_last_ran.assert_called_once_with(
        "totesys-transformed-data-bucket"
    )
    for table in TABLES:
        mock_list_all_filenames_in_s3.assert_any_call(
            Bucket=source_bucket,
            last_run_timestamp="test-last-timestamp",
            prefix=table,
        )
    mock_get_data_from_files.assert_called_once_with(
        Bucket=source_bucket, list_of_files="currency files"
    )
    mock_transform_to_dim_currency.assert_called_once_with(
        currency_data="currency_data"
    )
    mock_write_transformed_data_to_s3.assert_called_once_with(
        transform_bucket=transform_bucket,
        table_name="dim_currency",
        transformed_data="transformed_currency",
    )
    mock_update_last_ran_s3.assert_called_once_with(
        bucket_name=transform_bucket
    )


@patch("src.transform_handler.get_data_from_files")
@patch("src.transform_handler.list_all_filenames_in_s3")
@patch("src.transform_handler.update_last_ran_s3")
@patch("src.transform_handler.get_last_ran")
@patch("src.transform_handler.transform_to_dim_counterparty")
@patch("src.transform_handler.transform_to_dim_currency")
@patch("src.transform_handler.transform_to_dim_design")
@patch("src.transform_handler.transform_to_dim_location")
@patch("src.transform_handler.transform_to_dim_payment_type")
@patch("src.transform_handler.transform_to_dim_staff")
@patch("src.transform_handler.transform_to_dim_date")
@patch("src.transform_handler.transform_to_dim_transaction")
@patch("src.transform_handler.transform_to_fact_payment")
@patch("src.transform_handler.transform_to_fact_purchase_order")
@patch("src.transform_handler.transform_to_fact_sales_order")
@patch("src.transform_handler.filter_latest_data")
@patch("src.transform_handler.write_transformed_data_to_s3")
def test_transform_handler_works_when_all_utils_are_called(
    mock_write_transformed_data_to_s3,
    mock_filter_latest_data,
    mock_transform_to_fact_sales_order,
    mock_transform_to_fact_purchase_order,
    mock_transform_to_fact_payment,
    mock_transform_to_dim_transaction,
    mock_transform_to_dim_date,
    mock_transform_to_dim_staff,
    mock_transform_to_dim_payment_type,
    mock_transform_to_dim_location,
    mock_transform_to_dim_design,
    mock_transform_to_dim_currency,
    mock_transform_to_dim_counterparty,
    mock_get_last_ran,
    mock_update_last_ran_s3,
    mock_list_all_filenames_in_s3,
    mock_get_data_from_files,
):
    # Assign mock values
    mock_get_last_ran.return_value = "test-last-timestamp"

    def side_effect_list_files(Bucket, last_run_timestamp, prefix):
        if last_run_timestamp == "test-last-timestamp":
            return prefix + " files"
        if last_run_timestamp == first_timestamp:
            return prefix + " all files"

    mock_list_all_filenames_in_s3.side_effect = side_effect_list_files
    get_side_effect = [table + " data" for table in TABLES]
    get_side_effect.append("department all data")
    get_side_effect.append("address all data")
    mock_get_data_from_files.side_effect = get_side_effect

    mock_transform_to_dim_currency.return_value = "transformed_currency"
    mock_transform_to_fact_sales_order.return_value = "transformed_sales_order"
    mock_transform_to_fact_purchase_order.return_value = (
        "transformed_purchase_order"
    )
    mock_transform_to_fact_payment.return_value = "transformed_payment"
    mock_transform_to_dim_transaction.return_value = "transformed_transaction"
    mock_transform_to_dim_date.return_value = "transformed_date"
    mock_transform_to_dim_staff.return_value = "transformed_staff"
    mock_transform_to_dim_payment_type.return_value = (
        "transformed_payment_type"
    )
    mock_transform_to_dim_location.return_value = "transformed_location"
    mock_transform_to_dim_design.return_value = "transformed_design"
    mock_transform_to_dim_currency.return_value = "transformed_currency"
    mock_transform_to_dim_counterparty.return_value = (
        "transformed_counterparty"
    )

    mock_filter_latest_data.side_effect = [
        "department filtered data",
        "address filtered data",
    ]
    # Call function
    result = transform_handler("event", "context")

    # Assert values called correctly
    mock_get_last_ran.assert_called_once_with(
        "totesys-transformed-data-bucket"
    )
    for table in TABLES:
        mock_list_all_filenames_in_s3.assert_any_call(
            Bucket=source_bucket,
            last_run_timestamp="test-last-timestamp",
            prefix=table,
        )

        mock_get_data_from_files.assert_any_call(
            Bucket=source_bucket, list_of_files=table + " files"
        )

    for key, value in transformed_tables.items():
        mock_write_transformed_data_to_s3.assert_any_call(
            transform_bucket=transform_bucket,
            table_name=value,
            transformed_data="transformed_" + key,
        )

    mock_list_all_filenames_in_s3.assert_any_call(
        Bucket=source_bucket,
        last_run_timestamp=first_timestamp,
        prefix="department",
    )

    mock_list_all_filenames_in_s3.assert_any_call(
        Bucket=source_bucket,
        last_run_timestamp=first_timestamp,
        prefix="address",
    )

    mock_filter_latest_data.assert_any_call(
        data="department all data", key="department_id"
    )

    mock_filter_latest_data.assert_any_call(
        data="address all data", key="address_id"
    )

    mock_transform_to_dim_counterparty.assert_called_once_with(
        counterparty_data="counterparty data",
        address_data="address filtered data",
    )
    mock_transform_to_dim_currency.assert_called_once_with(
        currency_data="currency data"
    )
    mock_transform_to_dim_date.assert_called_once_with(
        sales_order_data="sales_order data", payment_data="payment data"
    )
    mock_transform_to_dim_design.assert_called_once_with(
        design_data="design data"
    )
    mock_transform_to_dim_location.assert_called_once_with(
        address_data="address data"
    )
    mock_transform_to_dim_payment_type.assert_called_once_with(
        payment_type_data="payment_type data"
    )
    mock_transform_to_dim_staff.assert_called_once_with(
        staff_data="staff data", department_data="department filtered data"
    )
    mock_transform_to_dim_transaction.assert_called_once_with(
        transaction_data="transaction data"
    )
    mock_transform_to_fact_payment.assert_called_once_with(
        payment_data="payment data"
    )
    mock_transform_to_fact_purchase_order.assert_called_once_with(
        purchase_order_data="purchase_order data"
    )
    mock_transform_to_fact_sales_order.assert_called_once_with(
        sales_order_data="sales_order data"
    )

    mock_update_last_ran_s3.assert_called_once_with(
        bucket_name=transform_bucket
    )

    assert result == {
        "statusCode": 200,
        "body": "Transformation went fine. Processed 158 files.",
    }
