import boto3
import re
from datetime import datetime
from botocore.exceptions import ClientError


def update_last_ran_s3(bucket_name="totesys-transformed-data-bucket"):
    """after processing update the last_ran file
    in s3 with the current timestamp"""
    s3_client = boto3.client("s3")
    current_time = datetime.now().isoformat()
    s3_client.put_object(
        Bucket=bucket_name, Key="last_ran.json", Body=current_time
    )


def get_last_ran(bucket_name):
    """retrieves timestamp from the last_ran file in s3,
    if the file doesn't exist it returns a default timestamp"""
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(
            Bucket=bucket_name, Key="last_ran.json"
        )
        last_ran = response["Body"].read().decode("utf-8")
        return datetime.fromisoformat(last_ran)
    except s3_client.exceptions.NoSuchKey:
        return datetime(1900, 1, 1)


def list_all_filenames_in_s3(Bucket, last_run_timestamp, prefix=""):
    """Find the names of all files in S3 bucket, which are newer than
    than last_ran, in the specified directory"""
    s3_client = boto3.client("s3")

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
                match = re.findall(r"\d{18}", file["Key"])
                if match:
                    timestamp = int(match[0])
                    if timestamp > last_run_timestamp:
                        file_names.append(file["Key"])
    # If no files exist under the prefix, raise a NameError
    if not all_files:
        raise NameError(f"No files found in s3://{Bucket}/{prefix}")

    # Return the list of files matching the timestamp criteria
    return file_names
