import json
import pytest
from src.transform_to_dim_currency import transform_to_dim_currency

currency_data_sample = [
    {
        "currency_id": 1,
        "currency_code": "USD",
        "created_at": "2024-11-13T12:00:00Z",
        "last_updated": "2024-11-13T12:00:00Z",
    },
    {
        "currency_id": 2,
        "currency_code": "EUR",
        "created_at": "2024-11-13T12:01:00Z",
        "last_updated": "2024-11-13T12:01:00Z",
    },
    {
        "currency_id": 3,
        "currency_code": "GBP",
        "created_at": "2024-11-13T12:02:00Z",
        "last_updated": "2024-11-13T12:02:00Z",
    },
]

expected_output = [
    {"currency_id": 1, "currency_code": "USD", "currency_name": "US Dollar"},
    {"currency_id": 2, "currency_code": "EUR", "currency_name": "Euro"},
    {
        "currency_id": 3,
        "currency_code": "GBP",
        "currency_name": "Pound Sterling",
    },
]


def test_dim_currency_happy_case():
    output = transform_to_dim_currency(currency_data_sample)
    expected_json_output = json.dumps(expected_output, separators=(",", ":"))

    assert output == expected_json_output


def test_returns_error_if_counterparty_data_is_empty():
    with pytest.raises(Exception, match="Error, currency_data is empty"):
        transform_to_dim_currency([])


def test_returns_error_if_currency_name_not_found():
    bad_data = [
        {
            "currency_id": 1,
            "currency_code": "kwehrkjasj",
            "created_at": "2024-11-13T12:00:00Z",
            "last_updated": "2024-11-13T12:00:00Z",
        }
    ]
    with pytest.raises(Exception, match="currency_name not found"):
        transform_to_dim_currency(bad_data)
