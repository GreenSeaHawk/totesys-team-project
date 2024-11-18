import boto3, json
from datetime import datetime
from botocore.exceptions import ClientError

def generate_timestamp():
    return datetime.now().strftime("%Y%m%d%H%M%S%f") # will have 6 decimal places
    # return datetime.now().strftime("%Y%m%d%H%M%S%f")[:-2] - can be used if we need to have 4 decimal

def add_to_s3_file_old(bucket, data, table):
    s3_client = boto3.client("s3")
    json_data = json.dumps(data)
    timestamp = generate_timestamp()
    # year = datetime.now().strftime("%Y")
    # month = datetime.now().strftime("%m")
    s3_key = f"{table}/{table}_{timestamp}.json" 
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=json_data)
    print(f'Object {s3_key} uploaded successfully to s3://{bucket}.')
    # except ClientError as e:
    #     error_message = f"Failed to upload data to {s3_key} to s3://{bucket}:{e.response['Error']['Message']}"
    #     raise Exception(error_message) from e

def list_all_objects_in_bucket(bucket_name):
    s3_client = boto3.client("s3")
    all_objects = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=continuation_token)
        else:
            response = s3_client.list_objects_v2(Bucket=bucket_name)

        if 'Contents' in response:
            all_objects.extend(response['Contents'])

        #check if more pages are available
        if response.get('IsTruncated'): #True if more objects are available
            continuation_token = response['NextContinuationToken']
        else:
            break
    return all_objects

def add_to_s3_file(bucket, data, table):
    s3_client = boto3.client("s3")
    try:
        json_data = json.dumps(data)
        timestamp = generate_timestamp()
        year = datetime.now().strftime("%Y")
        # month = datetime.now().strftime("%m")
        s3_key = f"{table}/{year}/{table}_{timestamp}.json" 
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=json_data)
        print(f'Object {s3_key} uploaded successfully to s3://{bucket}.')
    except ClientError as e:
        error_message = (
            f"Failed to upload data to {s3_key} to s3://{bucket}: "
            f"{e.response['Error']['Message']}"
        )
        raise Exception(error_message) from e