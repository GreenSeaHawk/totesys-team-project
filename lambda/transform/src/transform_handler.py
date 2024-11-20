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
from src.list_all_files import list_all_filenames_in_s3

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Helper function to carry out data transformation
def transform_data(data):
    transformed_data = [record for record in data if record.get('payment_type_id', 0)>3]
    return transformed_data

def lambda_handler(event, context):
    """Lambda Handler for the TRANSFORM part of the pipeline"""
    try:
        source_bucket = 'ingestion_bucket'
        transform_bucket = 'transform_bucket'
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
