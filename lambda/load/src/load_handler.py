from dbconnection import get_db_credentials, return_engine
from get_data_from_files import get_data_from_files
from list_all_filenames import list_all_filenames_in_s3
from upload_data_to_db import insert_data_to_postgres, update_last_ran_s3


"""PLAN:
invoke get_db_credentials
pass db_credentials into return_engine
we need a list of all the tables in the datawarehouse
loop through the list of tables
for each table:
    -files = list_all_filenames(bucket, table_name, load-last-ran.json)
    -data = get data from files(bucket, files)
    -insert_data_to_postgres(data, table_name, engine)
update_last_ran_s3(bucket, load-last-ran.json)
"""


def handler(event, context):
    list_of_tables = [
        "fact_sales_order",
        "fact_purchase_order",
        "fact_payment",
        "dim_counterparty",
        "dim_currency",
        "dim_date",
        "dim_location",
        "dim_staff",
        "dim_design",
        "dim_payment_type",
        "dim_transaction",
    ]
    key = "load-last-ran.json"
    bucket = "totesys-transformed-data-bucket"
    credentials = get_db_credentials("my-datawarehouse-connection")
    engine = return_engine(credentials)

    for table in list_of_tables:
        files = list_all_filenames_in_s3(bucket, table, key)
        data = get_data_from_files(bucket, files)
        insert_data_to_postgres(data, table, engine)

    update_last_ran_s3(bucket, key)
