# upload the data on the s3 bucket
# OPTIONAL - to generate unique file name with timestamp
# process table for ingestion - table has been uploaded to s3 bucket
# cross compares data from db to data in the ingestion zone. Stores all the changes.

import boto3
from botocore.exceptions import ClientError
from datetime import datetime

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

    #create last_ran file that stores timestamp of when our lambda function has been run
    #retrieve last_ran timestamp from s3
    #query the db for new or updated records:SELECT * FROM table_name WHERE last_updated OR created_at > last_ran
    #upload to s3 with current stamp

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

    
# ###############################

# def generate_unique_filename(table_name, extension="json"):
#     """Generate a unique filename with the table name and the current timestamp"""
#     timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
#     year = datetime.now().strftime("%Y")
#     month = datetime.now().strftime("%m")
#     return f"{table_name}/{year}/{month}/{table_name}_{timestamp}.{extension}"

