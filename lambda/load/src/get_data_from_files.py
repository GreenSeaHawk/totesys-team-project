import boto3
import pandas as pd
import io
import re


def get_data_from_files(bucket, files):
    s3 = boto3.client("s3")

    df = pd.DataFrame()
    for file in files:
        response = s3.get_object(Bucket=bucket, Key=file)
        buffer = io.BytesIO(response["Body"].read())
        df2 = pd.read_parquet(buffer)
        df = pd.concat([df, df2], ignore_index=True)
    if files:
        table_name = re.match(r"^[^/]+", files[0])[0]
        print(
            f"Succesfully captured {table_name} data from {len(files)} files"
        )
    else:
        print(f"No target files")

    return df
