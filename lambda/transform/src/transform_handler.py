# INPUT
# - Any other parameters (e.g. last_run.json key)
# - S3 bucket and prefixes for input and output

# WORKFLOW
# Step 1 = List all the relevant files
# Step 2 = Fetch and process the data from these files
# Step 3 = Transform the data (if required)
# Step 4 = Write the transformed data back to S3 bucket

# OUTPUT
# - Either Success or Failure
# - If Failure -> Handle the failure status / raise an error etc.
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError
from src.get_data_from_files import get_data_from_files
from src.add_to_s3_file import add_to_s3_file_parquet
from src.list_all_files import (
    list_all_filenames_in_s3,
    update_last_ran_s3,
    get_last_ran,
    generate_first_run_key,
)
from src.transform_to_dim_counterparty import transform_to_dim_counterparty
from src.transform_to_dim_currency import transform_to_dim_currency
from src.transform_to_fact_sales_order import transform_to_fact_sales_order
from src.transform_to_fact_payment import transform_to_fact_payment
from src.transform_to_dim_design import transform_to_dim_design
from src.transform_to_dim_location import transform_to_dim_location
from src.transform_to_dim_payment_type import transform_to_dim_payment_type
from src.transform_to_dim_staff import transform_to_dim_staff
from src.transform_to_dim_transaction import transform_to_dim_transaction
from src.transform_to_fact_purchase_order import (
    transform_to_fact_purchase_order,
)
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


def lambda_handler_2(event, context):
    """Lambda Handler for the TRANSFORM part of the pipeline"""
    try:
        # Step 1 - parameters for the lambda handler
        source_bucket = "totesys-data-bucket-cimmeria"
        transform_bucket = "totesys-transformed-data-bucket"
        last_timestamp = get_last_ran(transform_bucket)
        first_timestamp = datetime(1900, 1, 1)

        # Step 2 - define table-specific transformation functions
        table_transformation_map = {
            "counterparty": transform_to_dim_counterparty,
            "currency": transform_to_dim_currency,
            "date": transform_to_dim_date,
            "design": transform_to_dim_design,
            "location": transform_to_dim_location,
            "payment_type": transform_to_dim_payment_type,
            "staff": transform_to_dim_staff,
            "transaction": transform_to_dim_transaction,
            "payment": transform_to_fact_payment,
            "purchase_order": transform_to_fact_purchase_order,
            "sales_order": transform_to_fact_sales_order,
            "address" : transform_to_dim_location,
            "department": transform_to_dim_staff
        }

        # Step 3 - dictionaries to store filenames and data
        table_files = {}
        table_data = {}

        # fetch file names and data for all tables
        for table_name in table_transformation_map.keys():
            # get the filenames
            table_files[table_name] = list_all_filenames_in_s3(
                Bucket=source_bucket,
                last_run_timestamp=last_timestamp,
                prefix=table_name
            )
            if table_files[table_name]:
                # fetch the data
                table_data[table_name] = get_data_from_files(
                    Bucket=source_bucket, list_of_files=table_files[table_name]
                )
        # all the department data,
        all_department_files = list_all_filenames_in_s3(
            Bucket=source_bucket,
            last_run_timestamp=first_timestamp,
            prefix="department",
        )
        all_department_data = get_data_from_files(
            Bucket=source_bucket, list_of_files=all_department_files
        )
        filtered_department_data = filter_latest_data(data=all_department_data, key='department_id')
        # all the address data
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
        for table_name, transform_function in table_transformation_map.items():
            # some transformations may require additional data
            if table_name == "counterparty" and table_files['counterparty']:
                transformed_data = transform_function(
                    counterparty_data=table_data["counterparty"],
                    address_data=filtered_address_data,
                )
                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )

            elif table_name == "currency" and table_files['currency']:
                transformed_data = transform_function(currency_data=table_data["currency"])

                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )


            elif table_name == "date" and (table_files['sales_order'] or table_files['payment']):
                transformed_data = transform_function(
                    sales_order_data=table_data["sales_order"],
                    payment_data=table_data["payment"],
                )

                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )

            elif table_name == "design" and table_files['design']:
                transform_function(design_data=table_data["design"])

                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )

            elif table_name == "location" and table_files['address']:
                transform_function(address_data=table_data["location"])

                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )

            elif table_name == "payment_type" and table_files['payment_type']:
                transform_function(
                    payment_type_data=table_data["payment_type"]
                )

                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )

            elif table_name == "staff" and table_files['staff']:
                transform_function(
                    staff_data=table_data["staff"], 
                    department_data=filtered_department_data
                )
                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )
                
            elif table_name == "transaction" and table_files['transaction']:
                transform_function(
                    transaction_data=table_data["transaction"]
                )
                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )
                  

            elif table_name == "payment" and table_files['payment']:
                transform_function(payment_data=table_data["payment"])
                
                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )

            elif table_name == "purchase_order" and table_files['purchase_order']:
                transform_function(
                    purchase_order_data=table_data["purchase_order"]
                )
                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )

            elif table_name == "sales_order" and table_files['sales_order']:
                transform_function(
                    sales_order_data=table_data["sales_order"]
                )
                write_transformed_data_to_s3(
                        transform_bucket=transform_bucket,
                        table_name=table_name,
                        transformed_data=transformed_data
                )

        # update last_ran.json with the current timestamp
        update_last_ran_s3(bucket_name=transform_bucket)

        # count the total number of files processed
        total_files_processed = sum(
            len(files) for files in table_files.values()
        )

        return {
            "statusCode": 200,
            "body": f"Transformation went fine. Processed {total_files_processed} files.",
        }
    except ClientError as e:
        logger.error(f"ClientError: {e.response['Error']['Message']}")
        return {
            "statusCode": 500,
            "body": f"Client Error occured. {e.response['Error']['Message']}",
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"An unexpected Error occured. {str(e)}",
        }


def lambda_handler(event, context):
    """Lambda Handler for the TRANSFORM part of the pipeline"""
    try:
        source_bucket = "totesys-data-bucket-cimmeria"
        transform_bucket = "totesys-transformed-data-bucket"
        prefix = "payment_type"
        last_run_key = "last_run.json"

        # Step 1 = List all the relevant files
        logger.info("Listing all the files for transformation")
        file_names = list_all_filenames_in_s3(
            Bucket=source_bucket, prefix=prefix, Key=last_run_key
        )
        if not file_names:
            return {"statusCode": 200, "body": "Processed 0 files."}
        logger.info(
            f"Found {len(file_names)} file(s) to process: {file_names}"
        )

        # Step 2 = Fetch and process the data from these files
        logger.info("Fetching data from files")
        raw_data = get_data_from_files(
            Bucket=source_bucket, list_of_files=file_names
        )
        logger.info(f"Retrieved {len(raw_data)} records from the files.")

        # Step 3 = Perform any data transformation
        logger.info("Transforming data")
        transformed_data = transform_data(raw_data)

        # Step 4 = Write the transformed data back to S3 bucket
        logger.info("Writing transforming data to S3")
        add_to_s3_file_parquet(
            bucket=transform_bucket,
            data=transformed_data,
            table="payment_type",
        )
        logger.info("Transformation completed sucessfully.")

        # Step 5 - call the update_ran method to update last_ran.json file with the current timestamp
        update_last_ran_s3(bucket_name=transform_bucket)
        return {
            "statusCode": 200,
            "body": f"Transformation went fine. Processed {len(file_names)} files.",
        }
    except ClientError as e:
        logger.error(f"ClientError: {e.response['Error']['Message']}")
        return {
            "statusCode": 500,
            "body": f"Client Error occured. {e.response['Error']['Message']}",
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"An unexpected Error occured. {str(e)}",
        }
