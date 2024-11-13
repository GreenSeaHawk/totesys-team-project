import boto3
from pprint import pprint
import json


def get_data_from_files(Bucket, list_of_files):
    s3_client = boto3.client("s3")
    data = []
    for file in list_of_files:
        response = s3_client.get_object(Bucket=Bucket, Key=file)
        json_data= response["Body"].read().decode('utf-8')
        file_data = json.loads(json_data)
        data += file_data

    return data








# def get_data_from_files(Bucket,filnames):
#     '''Get the data in all files and store them in one variable'''
#     big_data = []
#             for file in filenames:
#                 response = s3.get_object(Bucket=IngestionBucket Key=file)
#                 data = json.load(response["body"])
#                 big_data.append(data)
#                 big_data list(set(big_data)) # to remove duplicates
#                 return big_data


