# establishing the database connection
# retrieve the last run timestamp from S3
# Iterating through the tables, extracting and serialising the data
# upload to the S3 bucket
# update the last_ran timstamp in S3
# close the db connection

import logging

from dbconnection import connect_to_db, close_db_connection
from extract_func import extract_table_data, serialise_data
from upload_data_s3 import (
    update_last_ran_s3,
    upload_raw_data_to_s3,
    get_last_ran,
)

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
TABLES = [
    "counterparty",
    "currency",
    "department",
    "design",
    "staff",
    "sales_order",
    "address",
    "payment",
    "purchase_order",
    "payment_type",
    "transaction",
]


def lambda_handler(event, context):
    BUCKET_NAME = "totesys_data_bucket_cimmeria"
    # establishing the database connection
    db_conn = connect_to_db()
    try:
        #  retrieve the last run timestamp from S3
        last_ran = get_last_ran(BUCKET_NAME)

        # fetch and process data from each table
        for table_name in TABLES:
            new_data = extract_table_data(db_conn, table_name, last_ran)

            if not new_data.empty:
                # serialise data to JSON format
                serialised_data = serialise_data(new_data, format="json")
                # upload the serialised data to s3 bucket
                upload_raw_data_to_s3(BUCKET_NAME, serialised_data, table_name)

        # update the last_ran timstamp in S3
        update_last_ran_s3(BUCKET_NAME)

    except Exception as e:
        logger.error(f"Error in lambda execution: {str(e)}")
        return {
            "statusCode": 500,
            "body": "Unfortunately, the lambda function failed.",
        }
    finally:
        if db_conn:
            close_db_connection(db_conn)

    logger.info("Data Ingestion went fine.")
    return {
        "statusCode": 200,
        "body": "Data Ingestion completed successfully."}

