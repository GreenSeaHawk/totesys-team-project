from src.extract_func import extract_table_data, serialise_data
import pytest
import pandas as pd
from unittest.mock import MagicMock

def test_extract_data_from_table():
    mock_connection = MagicMock()
    sample_data = [{
        "id" : 101,
        "name" : "charlie"
    },
    {
        "id" : 102,
        "name" : "jimmy"
    }]
    sample_df = pd.DataFrame(sample_data)
    pd.read_sql = MagicMock(return_value=sample_df)
    result = extract_table_data(mock_connection, "test-table")
    pd.read_sql.assert_called_once_with('SELECT * FROM test-table', mock_connection)
    assert isinstance(result, pd.DataFrame)
    assert result.equals(sample_df)

class TestSerialiseData:
    sample_data = pd.DataFrame({
        "id" : [101, 102],
        "name" : ["charlie", 'jimmy']
    }
    )
    def test_serialise_data_json(self):
        json_data = serialise_data(self.sample_data)
        assert isinstance(json_data, str)
        assert 'charlie' in json_data
        assert '102' in json_data
    
    def test_serialise_data_csv(self):
        csv_data = serialise_data(self.sample_data, format='csv')
        assert isinstance(csv_data, str)
        assert 'charlie' in csv_data
        assert '102' in csv_data

    def test_serialise_data_parquet(self, tmp_path):
        parquet_file = tmp_path/"test.parquet"
        self.sample_data.to_parquet(parquet_file)
        parquet_data = parquet_file.read_bytes()
        assert parquet_data

    def test_serialise_data_invalid_format(self):
        with pytest.raises(ValueError, match='unsupported format choose from either json, csv, or parquet'):
            serialise_data(self.sample_data, format='invalid')