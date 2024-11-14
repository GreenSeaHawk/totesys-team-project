# upload the data on the s3 bucket
# process table for ingestion - table has been uploaded to s3 bucket
# cross compares data from db to data in the ingestion zone. Stores all the changes.

import boto3
from botocore.exceptions import ClientError
from datetime import datetime

def upload_raw_data_to_s3(bucket_name, data, table_name):
    """Upload serialised data to S3 with a unique file name"""
    s3_client = boto3.client("s3")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    s3_key = f"{table_name}/{table_name}_{timestamp}.json" # organise the files in the ingestion_zone folder.
    try:
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=data, ContentType="application/json")
        print(f"Object {s3_key} uploaded successfully to bucket {bucket_name}.")
        return s3_key
    except ClientError as e:
        error_message = f"Failed to upload data to s3 at {s3_key}:{e.response['Error']['Message']}"
        raise Exception(error_message) from e

def get_last_ran(bucket_name):
    """retrieves timestamp from the last_ran file in s3, 
    if the file doesn't exist it returns a default timestamp"""
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key= "last_ran.txt")
        last_ran = response['Body'].read().decode('utf-8')
        return datetime.fromisoformat(last_ran)
    except s3_client.exceptions.NoSuchKey:
        return datetime(1900,1,1)
    
def update_last_ran_s3(bucket_name):
    """after processing update the last_ran file in s3 with the current timestamp"""
    s3_client = boto3.client("s3")
    current_time = datetime.now().isoformat()
    s3_client.put_object(Bucket=bucket_name, Key= "last_ran.txt", Body= current_time)



