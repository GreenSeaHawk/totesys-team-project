import logging
from datetime import datetime
from botocore.exceptions import ClientError
from src.get_data_from_files import get_data_from_files
from src.list_all_files import (
    list_all_filenames_in_s3,
    update_last_ran_s3,
    get_last_ran
)
from src.transform_to_dim_counterparty import transform_to_dim_counterparty
from src.transform_to_dim_currency import transform_to_dim_currency
from src.transform_to_dim_design import transform_to_dim_design
from src.transform_to_dim_location import transform_to_dim_location
from src.transform_to_dim_payment_type import transform_to_dim_payment_type
from src.transform_to_dim_staff import transform_to_dim_staff
from src.transform_to_dim_date import transform_to_dim_date
from src.transform_to_dim_transaction import transform_to_dim_transaction
from src.transform_to_fact_payment import transform_to_fact_payment
from src.transform_to_fact_purchase_order import (
    transform_to_fact_purchase_order,
)
from src.transform_to_fact_sales_order import transform_to_fact_sales_order
from src.remove_old_entries import filter_latest_data
from src.write_transfomed_data_to_s3 import write_transformed_data_to_s3

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

def transform_handler(event, context):
    """Lambda Handler for the TRANSFORM part of the pipeline"""
    # Step 1 - parameters for the lambda handler
    source_bucket = "totesys-data-bucket-cimmeria"
    transform_bucket = "totesys-transformed-data-bucket"
    last_timestamp = get_last_ran(transform_bucket)
    first_timestamp = datetime(1900, 1, 1)
    payment_data = []
    sales_order_data = []

    data_tables = [
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
        "transaction"
    ]
    # Latest files from each bucket
    table_data = {}
    for data_table_name in data_tables:
        list_of_new_files = list_all_filenames_in_s3(
                Bucket=source_bucket,
                last_run_timestamp=last_timestamp,
                prefix=data_table_name
            )
        if list_of_new_files:
            table_data[data_table_name] = get_data_from_files(
                    Bucket=source_bucket, 
                    list_of_files=list_of_new_files
                )

    # SPECIAL CASES WHERE WE NEED ALL THE LATEST DATA
    # all the department data only needed if staff data present
    if 'staff' in table_data.keys():
        all_department_files = list_all_filenames_in_s3(
            Bucket=source_bucket,
            last_run_timestamp=first_timestamp,
            prefix="department"
        )
        all_department_data = get_data_from_files(
            Bucket=source_bucket, list_of_files=all_department_files
        )
        filtered_department_data = filter_latest_data(data=all_department_data, key='department_id')
    # all the address data
    # only needed if there is counterparty data
    if 'counterparty' in table_data.keys():
        all_address_files = list_all_filenames_in_s3(
            Bucket=source_bucket,
            last_run_timestamp=first_timestamp,
            prefix="address",
        )
        all_address_data = get_data_from_files(
            Bucket=source_bucket, list_of_files=all_address_files
        )
        filtered_address_data = filter_latest_data(data=all_address_data, key='address_id')


    # apply transformations
    for table_name in table_data.keys():
        if table_name == "counterparty":
            transformed_data = transform_to_dim_counterparty(
                counterparty_data=table_data["counterparty"],
                address_data=filtered_address_data,
            )
            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='dim_counterparty',
                    transformed_data=transformed_data
            )

        if table_name == "currency":
            transformed_data = transform_to_dim_currency(
                currency_data=table_data["currency"]
                )

            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='dim_currency',
                    transformed_data=transformed_data
            )

        if table_name == "design":
            transformed_data = transform_to_dim_design(
                design_data=table_data["design"])

            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='dim_design',
                    transformed_data=transformed_data
            )

        if table_name == "address":
            transformed_data = transform_to_dim_location(
                address_data=table_data["address"])

            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='dim_location',
                    transformed_data=transformed_data
            )

        if table_name == "payment_type":
            transformed_data = transform_to_dim_payment_type(
                payment_type_data=table_data["payment_type"]
            )

            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='dim_payment_type',
                    transformed_data=transformed_data
            )

        if table_name == "staff":
            transformed_data = transform_to_dim_staff(
                staff_data=table_data["staff"], 
                department_data=filtered_department_data
            )
            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='dim_staff',
                    transformed_data=transformed_data
            )
            
        if table_name == "transaction":
            transformed_data = transform_to_dim_transaction(
                transaction_data=table_data["transaction"]
            )
            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='dim_transaction',
                    transformed_data=transformed_data
            )
                

        if table_name == "payment":
            payment_data = table_data["payment"]
            transformed_data = transform_to_fact_payment(
                payment_data=table_data["payment"])
            
            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='fact_payment',
                    transformed_data=transformed_data
            )

        if table_name == "purchase_order":
            transformed_data = transform_to_fact_purchase_order(
                purchase_order_data=table_data["purchase_order"]
            )
            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='fact_purchase_order',
                    transformed_data=transformed_data
            )

        if table_name == "sales_order":
            sales_order_data = table_data["sales_order"]
            transformed_data = transform_to_fact_sales_order(
                sales_order_data=table_data["sales_order"]
            )
            write_transformed_data_to_s3(
                    transform_bucket=transform_bucket,
                    table_name='fact_sales_order',
                    transformed_data=transformed_data
            )
    # Run dim date with data if it exists
    if sales_order_data or payment_data:
        transformed_data = transform_to_dim_date(
                sales_order_data=sales_order_data,
                payment_data=payment_data
            )
        write_transformed_data_to_s3(
                transform_bucket=transform_bucket,
                table_name='dim_date',
                transformed_data=transformed_data
            )

    # update last_ran.json with the current timestamp
    update_last_ran_s3(bucket_name=transform_bucket)

    # count the total number of files processed
    total_files_processed = sum(
        len(files) for files in table_data.values()
    )

    return {
        "statusCode": 200,
        "body": f"Transformation went fine. Processed {total_files_processed} files.",
    }

    # except ClientError as e:
    #     logger.error(f"ClientError: {e.response['Error']['Message']}")
    #     return {
    #         "statusCode": 500,
    #         "body": f"Client Error occured. {e.response['Error']['Message']}",
    #     }
    # except Exception as e:
    #     logger.error(f"Error: {str(e)}")
    #     return {
    #         "statusCode": 500,
    #         "body": f"An unexpected Error occured. {str(e)}",
    #     }
