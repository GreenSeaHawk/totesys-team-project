import boto3


def list_all_filenames_in_s3(Bucket, prefix=''):
    '''Find the names of all files in S3 bucket, which are newer than than last_ran, in the specified directory'''
    pass



# def list_all_filenames_in_s3(Bucket, prefix=''):
# '''Find the names of all files in S3 bucket, which are newer than than last_ran'''
#     s3_client = boto3.client('s3')
#     paginator = s3_client.get_paginator('list_objects_v2')
#     pages = paginator.paginate(Bucket=Bucket, Prefix=prefix)
    
#     filenames = []  # List to store all file names
    
#     for page in pages:
#         if 'Contents' in page:
#             for obj in page['Contents']:
#                 if obj['key'](exctract date from this) > last_run
#                 filenames.append(obj['Key'])  # Add each file's key (file name) to the list
    
#     return filename