import logging
from src.add_to_s3_file import add_to_s3_file_parquet

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def write_transformed_data_to_s3(
    transform_bucket, table_name, transformed_data
):
    try:
        logger.info(f"Writing transforming data for table {table_name} to S3")
        add_to_s3_file_parquet(
            bucket=transform_bucket, data=transformed_data, table=table_name
        )
        logger.info(
            f"Transformation completed sucessfully for table {table_name}."
        )
    except Exception as e:
        logger.error(
            f"Failed to write transformed data for table {table_name} to s3: {str(e)}"
        )
        raise e
