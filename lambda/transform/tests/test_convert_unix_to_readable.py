from src.convert_unix_to_readable import convert_unix_to_readable

def test_unix_timestamp_converts_to_readable():
    unix_timestamp = 1731947889824
    expected_outcome = "2024-11-18 16:38:09"
    result = convert_unix_to_readable(unix_timestamp)

    assert result == expected_outcome
