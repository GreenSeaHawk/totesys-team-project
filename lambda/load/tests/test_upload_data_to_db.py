import pytest
import pandas as pd
import boto3
from src.dbconnection import return_engine
from src.upload_data_to_db import (
    insert_data_to_postgres,
    update_last_ran_s3,
    postgres_upsert,
)
from unittest.mock import patch
from datetime import datetime
from moto import mock_aws

# Variables
LAST_RAN_KEY = "load_last_ran.json"
BUCKET_NAME = "test_bucket"


@pytest.fixture
def setup_s3_bucket():
    """Mock the S3 bucket for testing with moto."""
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3


class TestInsertToPostgres:
    @patch("pandas.DataFrame.to_sql")
    def test_insert_data_passes_correct_args_to_to_sql(self, mock_to_sql):
        credentials = {
            "user": "test_user",
            "password": "test_pass",
            "host": "my_host",
            "database": "my_database",
            "port": 1000,
        }
        table_name = "dummy_table"
        df = pd.DataFrame()
        engine = return_engine(credentials)

        insert_data_to_postgres(df, table_name, engine)

        mock_to_sql.assert_called_once_with(
            table_name,
            engine,
            if_exists="append",
            index=False,
            method=postgres_upsert,
        )

    @patch("pandas.DataFrame.to_sql")
    def test_insert_data_prints_correct_string(self, mock_to_sql, capsys):
        credentials = {
            "user": "test_user",
            "password": "test_pass",
            "host": "my_host",
            "database": "my_database",
            "port": 1000,
        }
        table_name = "dummy_table"
        df = pd.DataFrame()
        engine = return_engine(credentials)

        insert_data_to_postgres(df, table_name, engine)

        capture = capsys.readouterr()

        assert capture.out.strip() == f"Inserted 0 rows into {table_name}."

    def test_invoking_function_with_no_patch_raises_error(self):
        credentials = {
            "user": "test_user",
            "password": "test_pass",
            "host": "my_host",
            "database": "my_database",
            "port": 1000,
        }
        table_name = "dummy_table"
        df = pd.DataFrame()
        engine = return_engine(credentials)

        with pytest.raises(Exception):
            insert_data_to_postgres(df, table_name, engine)


class TestUpdateLastRan:
    def test_update_last_ran_puts_current_time(self, setup_s3_bucket):

        update_last_ran_s3(BUCKET_NAME, Key=LAST_RAN_KEY)

        response = setup_s3_bucket.get_object(
            Bucket=BUCKET_NAME, Key=LAST_RAN_KEY
        )
        stored_time = response["Body"].read().decode("utf-8")
        current_time = datetime.fromisoformat(stored_time)
        assert (datetime.now() - current_time).total_seconds() < 3
