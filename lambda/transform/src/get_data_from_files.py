import boto3
from pprint import pprint
import json


def get_data_from_files(Bucket, list_of_files):
    s3_client = boto3.client("s3")
    data = []
    for file in list_of_files:
        response = s3_client.get_object(Bucket=Bucket, Key=file)
        json_data = response["Body"].read().decode("utf-8")
        file_data = json.loads(json_data)
        data += file_data

    return data
