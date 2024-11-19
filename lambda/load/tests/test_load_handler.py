from src.load_handler import handler
from unittest.mock import patch

list_of_tables = [
    "fact_sales_order",
    "fact_purchase_order",
    "fact_payment",
    "dim_counterparty",
    "dim_currency",
    "dim_date",
    "dim_location",
    "dim_staff",
    "dim_design",
    "dim_payment_type",
    "dim_transaction"
    ]
bucket = "totesys-transformed-data-bucket"
key = 'load-last-ran.json'

@patch("src.load_handler.get_db_credentials")
@patch("src.load_handler.return_engine")
@patch("src.load_handler.get_data_from_files")
@patch("src.load_handler.list_all_filenames_in_s3")
@patch("src.load_handler.insert_data_to_postgres")
@patch("src.load_handler.update_last_ran_s3")
def test_handler_invokes_utils_correctly(
    mock_update_last_ran_s3,
    mock_insert_data_to_postgres,
    mock_list_all_filenames_in_s3,
    mock_get_data_from_files,
    mock_return_engine,
    mock_get_db_credentials
):
    mock_get_db_credentials.return_value = 'test-credentials'
    mock_return_engine.return_value = 'test-engine'
    mock_list_all_filenames_in_s3.side_effect = [
        table + ' files' for table in list_of_tables
    ]
    mock_get_data_from_files.side_effect = [
        table + ' data' for table in list_of_tables
    ]

    handler("event", "context")

    mock_get_db_credentials.assert_called_once_with('my-datawarehouse-connection')
    mock_return_engine.assert_called_once_with('test-credentials')
    
    for table in list_of_tables:

        mock_list_all_filenames_in_s3.assert_any_call(bucket, table, key)

        mock_get_data_from_files.assert_any_call(bucket, f'{table} files')
        mock_insert_data_to_postgres.assert_any_call(f'{table} data', table, 'test-engine')

    mock_update_last_ran_s3.assert_called_once_with(bucket, key)
    