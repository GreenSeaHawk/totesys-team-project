import json
import pytest
from src.transform_to_dim_transaction import transform_to_dim_transaction

transaction_sample_data = [
    {
        "transaction_id": 1,
        "transaction_type": "SALE",
        "sales_order_id": 1001,
        "purchase_order_id": None,
        "created_at": "2024-01-05T08:00:00Z",
        "last_updated": "2024-01-05T08:00:00Z",
    },
    {
        "transaction_id": 2,
        "transaction_type": "PURCHASE",
        "sales_order_id": None,
        "purchase_order_id": 2002,
        "created_at": "2024-01-06T09:00:00Z",
        "last_updated": "2024-01-06T09:00:00Z",
    },
    {
        "transaction_id": 3,
        "transaction_type": "SALE",
        "sales_order_id": 1003,
        "purchase_order_id": None,
        "created_at": "2024-01-07T10:30:00Z",
        "last_updated": "2024-01-07T10:30:00Z",
    },
]

expected_output = [
    {
        "transaction_id": 1,
        "transaction_type": "SALE",
        "sales_order_id": 1001,
        "purchase_order_id": None,
    },
    {
        "transaction_id": 2,
        "transaction_type": "PURCHASE",
        "sales_order_id": None,
        "purchase_order_id": 2002,
    },
    {
        "transaction_id": 3,
        "transaction_type": "SALE",
        "sales_order_id": 1003,
        "purchase_order_id": None,
    },
]


def test_dim_transaction_happy_case():
    output = transform_to_dim_transaction(transaction_sample_data)
    expected_json_output = json.dumps(expected_output, separators=(",", ":"))

    assert output == expected_json_output


def test_returns_error_if_transaction_data_is_empty():
    with pytest.raises(Exception, match="Error, transaction_data is empty"):
        transform_to_dim_transaction([])
