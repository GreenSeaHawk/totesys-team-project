import pytest
import pandas as pd
from src.dbconnection import return_engine
from src.upload_data_to_db import insert_data_to_postgres
from unittest.mock import patch

class TestInsertToPostgres:
    @patch("upload_data_to_db.pd.DataFrame.to_sql")
    def test_insert_data_passes_correct_args_to_to_sql(self, mock_to_sql):
        credentials = {
            "user": "test_user",
            "password": "test_pass",
            "host": "my_host",
            "database": "my_database",
            "port": 1000,
        }
        table_name = 'dummy_table'
        df = pd.DataFrame()
        engine = return_engine(credentials)

        insert_data_to_postgres(df, table_name, engine)

        mock_to_sql.assert_called_once_with(table_name, engine, if_exists='append', index=False)
    
    @patch("upload_data_to_db.pd.DataFrame.to_sql")
    def test_insert_data_prints_correct_string(self, mock_to_sql, capsys):
        credentials = {
            "user": "test_user",
            "password": "test_pass",
            "host": "my_host",
            "database": "my_database",
            "port": 1000,
        }
        table_name = 'dummy_table'
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
        table_name = 'dummy_table'
        df = pd.DataFrame()
        engine = return_engine(credentials)
        
        with pytest.raises(Exception):
            insert_data_to_postgres(df, table_name, engine)


   