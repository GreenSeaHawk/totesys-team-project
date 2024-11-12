# upload the data on the s3 bucket
# OPTIONAL - to generate unique file name with timestamp
# process table for ingestion - table has been uploaded to s3 bucket
# cross compares data from db to data in the ingestion zone. Stores all the changes.

import boto3
from botocore.exceptions import ClientError
from datetime import datetime

def generate_unique_filename(table_name, extension="json"):
    """Generate a unique filename with the table name and the current timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    return f"{table_name}/{year}/{month}/{table_name}_{timestamp}.{extension}"

def upload_raw_data_to_s3(bucket_name, data, table_name):
    """Upload serialised data to S3 with a unique file name"""
    s3_client = boto3.client("s3")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    year = datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    s3_key = f"{table_name}/{year}/{month}/{table_name}_{timestamp}.json" # organise the files in the ingestion_zone folder.
    try:
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=data, ContentType="application/json")
        print(f"Object {s3_key} uploaded successfully to bucket {bucket_name}.")
        return s3_key
    except ClientError as e:
        error_message = f"Failed to upload data to s3 at {s3_key}:{e.response['Error']['Message']}"
        raise Exception(error_message) from e

def cross_compare_data(database_data, s3_data):
    """Compare the data from the database to the data in the ingestion zone on the s3 bucket."""
    if s3_data is None:
        print("Sorry, no S3 data available for comparison.")
        return None

    #Identify the differences (assuming both are dataframes)
    # comparison_df 
    # new_records which were NOT already present in s3
    # missing_records which are no longer present in the database

    # return new_records, missing_records
