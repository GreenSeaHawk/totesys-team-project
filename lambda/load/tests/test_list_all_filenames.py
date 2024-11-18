

# @pytest.fixture
# def transform_bucket_2022(s3, create_transform_bucket):
#     file_key = "last_run_load.json"
#     file_content = "20210101000000"
#     s3.put_object(Bucket="transform_bucket", Key=file_key, Body=file_content)


# @pytest.fixture
# def transform_bucket_2022_dec(s3, create_transform_bucket):
#     file_key = "last_run_load.json"
#     file_content = "20221201143000"
#     s3.put_object(Bucket="transform_bucket", Key=file_key, Body=file_content)


# @pytest.fixture
# def transform_bucket_2025(s3, create_transform_bucket):
#     file_key = "last_run_load.json"
#     file_content = "20251201143000"
#     s3.put_object(Bucket="transform_bucket", Key=file_key, Body=file_content)