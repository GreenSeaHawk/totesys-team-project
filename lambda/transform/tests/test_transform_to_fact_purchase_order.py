import json
import pytest
from src.transform_to_fact_purchase_order import transform_to_fact_purchase_order


purchase_order_data_sample = [
  {
    "purchase_order_id": 1,
    "created_at": "2024-11-15T10:30:00",
    "last_updated": "2024-11-15T10:30:00",
    "staff_id": 101,
    "counterparty_id": 2001,
    "item_code": "ABC123",
    "item_quantity": 150,
    "item_unit_price": 75.50,
    "currency_id": 1,
    "agreed_delivery_date": "2024-11-25",
    "agreed_payment_date": "2024-11-20",
    "agreed_delivery_location_id": 5001
  },
  {
    "purchase_order_id": 2,
    "created_at": "2024-11-15T11:45:00",
    "last_updated": "2024-11-15T11:45:00",
    "staff_id": 102,
    "counterparty_id": 2002,
    "item_code": "XYZ789",
    "item_quantity": 500,
    "item_unit_price": 30.00,
    "currency_id": 2,
    "agreed_delivery_date": "2024-12-01",
    "agreed_payment_date": "2024-11-22",
    "agreed_delivery_location_id": 5002
  },
  {
    "purchase_order_id": 3,
    "created_at": "2024-11-15T12:00:00",
    "last_updated": "2024-11-15T12:00:00",
    "staff_id": 103,
    "counterparty_id": 2003,
    "item_code": "LMN456",
    "item_quantity": 1000,
    "item_unit_price": 45.25,
    "currency_id": 3,
    "agreed_delivery_date": "2024-11-30",
    "agreed_payment_date": "2024-11-25",
    "agreed_delivery_location_id": 5003
  }
]

expected_output = [
  {
    "purchase_record_id": 1,
    "purchase_order_id": 1,
    "created_date": "2024-11-15",
    "created_time": "10:30:00",
    "last_updated_date": "2024-11-15",
    "last_updated_time": "10:30:00",
    "staff_id": 101,
    "counterparty_id": 2001,
    "item_code": "ABC123",
    "item_quantity": 150,
    "item_unit_price": 75.5,
    "currency_id": 1,
    "agreed_delivery_date": "2024-11-25",
    "agreed_payment_date": "2024-11-20",
    "agreed_delivery_location_id": 5001
  },
  {
    "purchase_record_id": 2,
    "purchase_order_id": 2,
    "created_date": "2024-11-15",
    "created_time": "11:45:00",
    "last_updated_date": "2024-11-15",
    "last_updated_time": "11:45:00",
    "staff_id": 102,
    "counterparty_id": 2002,
    "item_code": "XYZ789",
    "item_quantity": 500,
    "item_unit_price": 30.0,
    "currency_id": 2,
    "agreed_delivery_date": "2024-12-01",
    "agreed_payment_date": "2024-11-22",
    "agreed_delivery_location_id": 5002
  },
  {
    "purchase_record_id": 3,
    "purchase_order_id": 3,
    "created_date": "2024-11-15",
    "created_time": "12:00:00",
    "last_updated_date": "2024-11-15",
    "last_updated_time": "12:00:00",
    "staff_id": 103,
    "counterparty_id": 2003,
    "item_code": "LMN456",
    "item_quantity": 1000,
    "item_unit_price": 45.25,
    "currency_id": 3,
    "agreed_delivery_date": "2024-11-30",
    "agreed_payment_date": "2024-11-25",
    "agreed_delivery_location_id": 5003
  }
]



def test_to_fact_purchase_order_happy_case():
    output = transform_to_fact_purchase_order(purchase_order_data_sample)
    expected_json_output = json.dumps(expected_output, separators=(',',':'))
    print(output)
    assert output == expected_json_output

def test_returns_error_if_purchase_order_data_is_empty():
    with pytest.raises(Exception, match='Error, purchase_order_data is empty'):
        transform_to_fact_purchase_order([])