import json
import pytest
from src.transform_to_dim_payment_type import transform_to_dim_payment_type

payment_type_sample_data = [
    {
        "payment_type_id": 1,
        "payment_type_name": "SALES_RECEIPT",
        "created_at": "2024-01-01T08:00:00Z",
        "last_updated": "2024-01-01T08:00:00Z",
    },
    {
        "payment_type_id": 2,
        "payment_type_name": "PURCHASE_PAYMENT",
        "created_at": "2024-01-02T09:00:00Z",
        "last_updated": "2024-01-02T09:00:00Z",
    },
    {
        "payment_type_id": 3,
        "payment_type_name": "REFUND",
        "created_at": "2024-01-03T10:00:00Z",
        "last_updated": "2024-01-03T10:00:00Z",
    },
]

expected_output = [
    {"payment_type_id": 1, "payment_type_name": "SALES_RECEIPT"},
    {"payment_type_id": 2, "payment_type_name": "PURCHASE_PAYMENT"},
    {"payment_type_id": 3, "payment_type_name": "REFUND"},
]


def test_dim_payment_type_happy_case():
    output = transform_to_dim_payment_type(payment_type_sample_data)
    expected_json_output = json.dumps(expected_output, separators=(",", ":"))

    assert output == expected_json_output


def test_returns_error_if_payment_type_data_is_empty():
    with pytest.raises(Exception, match="Error, payment_type_data is empty"):
        transform_to_dim_payment_type([])
