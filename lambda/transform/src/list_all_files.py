import boto3
from pprint import pprint
import re

def list_all_filenames_in_s3(Bucket,prefix=''):
    '''Find the names of all files in S3 bucket, which are newer than than last_ran, in the specified directory'''
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket="transform_bucket", Key="last_run.json")
    last_run_timestamp = int(response["Body"].read().decode('utf-8'))
    
    files = s3_client.list_objects_v2(Bucket=Bucket, Prefix=prefix)
    file_names = []
    try:
        for file in files['Contents']:
            timestamp=int(re.findall(r'\d+', file["Key"])[0])
            if timestamp > last_run_timestamp:
                file_names.append(file['Key'])
    except KeyError:
        raise NameError(f"no files in s3://{Bucket}/{prefix}")
    
    return file_names

def list_all_filenames_in_s3_version2(Bucket,prefix=''):
    '''Find the names of all files in S3 bucket, which are newer than than last_ran, in the specified directory'''
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket="transform_bucket", Key="last_run.json")
    last_run_timestamp = int(response["Body"].read().decode('utf-8'))

    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=Bucket, Prefix=prefix)
    
    # files = s3_client.list_objects_v2(Bucket=Bucket, Prefix=prefix)
    file_names = []
    try:
        for page in page_iterator:
            if 'Contents' in page:
                for file in page['Contents']:
                    match = re.findall(r"\d+", file["Key"])
                    if match:
                        timestamp = int(match[0])
                        if timestamp > last_run_timestamp:
                            file_names.append(file['Key']) 
    except KeyError:
        raise NameError(f"no files in s3://{Bucket}/{prefix}")
    
    return file_names


