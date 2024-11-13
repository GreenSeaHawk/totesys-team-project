import boto3, json
from datetime import datetime

def add_to_s3_file(bucket, data, table):
    s3_client = boto3.client("s3")
    json_data = json.dumps(data)
    s3_client.put_object(Bucket=bucket, Key='address/test', Body=json_data)




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



# def add_to_s3_file(Bucket, Key, additional_content):
#     s3_client = boto3.client('s3')
    
#     # Step 1: Download the existing file content
#     response = s3_client.get_object(Bucket=bucket_name, Key=key)
#     existing_content = json.load(response["body"])
    
#     # Step 2: Add new content to the existing content
#     updated_content = existing_content + additional_content
    
#     # Step 3: Upload the updated content back to S3
#     s3_client.put_object(Bucket=bucket_name, Key=key, Body=updated_content)