import boto3, json
from datetime import datetime
from botocore.exceptions import ClientError

def add_to_s3_file(bucket, data, table):
    s3_client = boto3.client("s3")
    json_data = json.dumps(data)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    s3_key = f"{table}/{table}_{timestamp}.json" 
    s3_client.put_object(Bucket=bucket, Key=s3_key, Body=json_data)
    print(f'Object {s3_key} uploaded successfully to s3://{bucket}.')
    # except ClientError as e:
    #     error_message = f"Failed to upload data to {s3_key} to s3://{bucket}:{e.response['Error']['Message']}"
    #     raise Exception(error_message) from e
