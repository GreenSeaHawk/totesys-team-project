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
from botocore.exceptions import ClientError
from src.get_data_from_files import get_data_from_files
from src.add_to_s3_file import add_to_s3_file_parquet, add_to_s3_file_json
from src.list_all_files import list_all_filenames_in_s3, update_last_ran_s3
from src.transform_to_dim_counterparty import transform_to_dim_counterparty

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

# Helper function to carry out data transformation
def transform_data(data):
    transformed_data = [record for record in data if record.get('payment_type_id', 0)>3]
    return transformed_data

def transform_data_2(data, table_name, additional_data=None):
    if table_name == 'counterparty':
        address_data = additional_data.get('address')
        return transform_to_dim_counterparty(data, address_data=address_data)
    elif table_name == 'currency':
        return transform_to_dim_counterparty(data, address_data=address_data)

def lambda_handler_2(event, context):
    """Lambda Handler for the TRANSFORM part of the pipeline"""
    try:
        source_bucket = 'totesys-data-bucket-cimmeria'
        transform_bucket = 'totesys-transformed-data-bucket'
        last_run_key = 'last_run.json'

        # dictionary to hold any additional data
        additional_data = {}

        for table in TABLES:
            logger.info(f'Processing table: {table}')
            file_names = list_all_filenames_in_s3(Bucket=source_bucket, prefix=table, Key=last_run_key)

            if not file_names:
                logger.info(f'No files found for: {table}')
                continue

            raw_data = get_data_from_files(Bucket=source_bucket, list_of_files=file_names)

            # save the address data for dependant transormations
            if table == 'address':
                additional_data['address'] = raw_data

            # transform data
            transformed_data = transform_data(data=raw_data, table_name=table, additional_data=additional_data)

            # upload transformed data to s3
            add_to_s3_file_parquet(
                bucket=transform_bucket,
                data=transformed_data,
                table=table
            )

            logger.info(f'Successfully processed: {table}')
        # Step 5 - call the update_ran method to update last_ran.json file with the current timestamp
        update_last_ran_s3(bucket_name=transform_bucket)
        return {
            "statusCode":200,
            "body":f"Transformation went fine. Processed {len(file_names)} files."
        }
    except ClientError as e:
        logger.error(f"ClientError: {e.response['Error']['Message']}")
        return {
            "statusCode":500,
            "body":f"Client Error occured. {e.response['Error']['Message']}"
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            "statusCode":500,
            "body":f"An unexpected Error occured. {str(e)}"
        }

def lambda_handler(event, context):
    """Lambda Handler for the TRANSFORM part of the pipeline"""
    try:
        source_bucket = 'totesys-data-bucket-cimmeria'
        transform_bucket = 'totesys-transformed-data-bucket'
        prefix = 'payment_type'
        last_run_key = 'last_run.json'

        # Step 1 = List all the relevant files
        logger.info("Listing all the files for transformation")
        file_names = list_all_filenames_in_s3(Bucket=source_bucket, prefix=prefix, Key=last_run_key)
        if not file_names:
            return {
                "statusCode": 200,
                "body": "Processed 0 files."
            }
        logger.info(f"Found {len(file_names)} file(s) to process: {file_names}")

        # Step 2 = Fetch and process the data from these files
        logger.info("Fetching data from files")
        raw_data = get_data_from_files(Bucket=source_bucket, list_of_files=file_names)
        logger.info(f"Retrieved {len(raw_data)} records from the files.")

        # Step 3 = Perform any data transformation
        logger.info("Transforming data")
        transformed_data = transform_data(raw_data)

        # Step 4 = Write the transformed data back to S3 bucket
        logger.info("Writing transforming data to S3")
        add_to_s3_file_parquet(bucket=transform_bucket, data=transformed_data, table='payment_type')
        logger.info("Transformation completed sucessfully.")

        # Step 5 - call the update_ran method to update last_ran.json file with the current timestamp
        update_last_ran_s3(bucket_name=transform_bucket)
        return {
            "statusCode":200,
            "body":f"Transformation went fine. Processed {len(file_names)} files."
        }
    except ClientError as e:
        logger.error(f"ClientError: {e.response['Error']['Message']}")
        return {
            "statusCode":500,
            "body":f"Client Error occured. {e.response['Error']['Message']}"
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            "statusCode":500,
            "body":f"An unexpected Error occured. {str(e)}"
        }
