import json
import pytest
from src.transform_to_fact_payment import transform_to_fact_payment


payment_type_sample_data = [
  {
    "payment_id": 1,
    "created_at": "2024-11-10T08:00:00Z",
    "last_updated": "2024-11-10T08:00:00Z",
    "transaction_id": 101,
    "counterparty_id": 1,
    "payment_amount": 5000.75,
    "currency_id": 1,
    "payment_type_id": 1,
    "paid": True,
    "payment_date": "2024-11-10",
    "company_ac_number": 12345678,
    "counterparty_ac_number": 87654321
  },
  {
    "payment_id": 2,
    "created_at": "2024-11-15T10:30:00Z",
    "last_updated": "2024-11-15T10:30:00Z",
    "transaction_id": 102,
    "counterparty_id": 2,
    "payment_amount": 25000.00,
    "currency_id": 2,
    "payment_type_id": 2,
    "paid": False,
    "payment_date": "2024-12-01",
    "company_ac_number": 23456789,
    "counterparty_ac_number": 98765432
  }
]

expected_output = [
  {
    "payment_record_id": 1,
    "payment_id": 1,
    "created_date": "2024-11-10",
    "created_time": "08:00:00",
    "last_updated_date": "2024-11-10",
    "last_updated_time": "08:00:00",
    "transaction_id": 101,
    "counterparty_id": 1,
    "payment_amount": 5000.75,
    "currency_id": 1,
    "payment_type_id": 1,
    "paid": True,
    "payment_date": "2024-11-10"
  },
  {
    "payment_record_id": 2,
    "payment_id": 2,
    "created_date": "2024-11-15",
    "created_time": "10:30:00",
    "last_updated_date": "2024-11-15",
    "last_updated_time": "10:30:00",
    "transaction_id": 102,
    "counterparty_id": 2,
    "payment_amount": 25000.0,
    "currency_id": 2,
    "payment_type_id": 2,
    "paid": False,
    "payment_date": "2024-12-01"
  }
]

def test_fact_payment_happy_case():
    output = transform_to_fact_payment(payment_type_sample_data)
    expected_json_output = json.dumps(expected_output, separators=(',',':'))

    assert output == expected_json_output

def test_returns_error_if_payment_data_is_empty():
    with pytest.raises(Exception, match='Error, payment_data is empty'):
        transform_to_fact_payment([])