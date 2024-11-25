import boto3
import re

from botocore.exceptions import ClientError


def list_all_filenames_in_s3(Bucket, prefix="", Key="last_run.json"):
    """Find the names of all files in S3 bucket, which are newer than
    than last_ran, in the specified directory"""
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(Bucket="transform_bucket", Key=Key)
        last_run_timestamp = int(response["Body"].read().decode("utf-8"))
    except ClientError as e:
        raise Exception(
            f"Error retrieving {last_run_timestamp}: "
            f"{e.response['Error']['Message']}"
        ) from e

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=Bucket, Prefix=prefix)
    except ClientError as e:
        raise Exception(
            f"Error accessing S3 bucket '{Bucket}' with prefix '{prefix}':"
            f"{e.response['Error']['Message']}"
        ) from e

    # Collect files
    all_files = []
    file_names = []
    for page in page_iterator:
        if "Contents" in page:
            for file in page["Contents"]:
                all_files.append(file["Key"])
                match = re.findall(r"\d+", file["Key"])
                if match:
                    timestamp = int(match[0])
                    if timestamp > last_run_timestamp:
                        file_names.append(file["Key"])
    # If no files exist under the prefix, raise a NameError
    if not all_files:
        raise NameError(f"No files found in s3://{Bucket}/{prefix}")

    # Return the list of files matching the timestamp criteria
    return file_names
