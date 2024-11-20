import json
import pytest
from src.transform_to_fact_sales_order import transform_to_fact_sales_order

sales_sample_data = [
    {
        "sales_order_id": 1,
        "created_at": "2024-11-13T12:00:00Z",
        "last_updated": "2024-11-13T12:00:00Z",
        "design_id": 101,
        "staff_id": 10,
        "counterparty_id": 1,
        "units_sold": 5000,
        "unit_price": 3.50,
        "currency_id": 1,
        "agreed_delivery_date": "2024-12-01",
        "agreed_payment_date": "2024-12-05",
        "agreed_delivery_location_id": 20
    },
    {
        "sales_order_id": 2,
        "created_at": "2024-01-13T12:05:00Z",
        "last_updated": "2024-04-13T12:05:00Z",
        "design_id": 102,
        "staff_id": 12,
        "counterparty_id": 2,
        "units_sold": 15000,
        "unit_price": 2.75,
        "currency_id": 2,
        "agreed_delivery_date": "2024-12-10",
        "agreed_payment_date": "2024-12-15",
        "agreed_delivery_location_id": 21
    }
]

expected_output = [
  {
    "sales_record_id": 1,
    "sales_order_id": 1,
    "created_date": "2024-11-13",
    "created_time": "12:00:00",
    "last_updated_date": "2024-11-13",
    "last_updated_time": "12:00:00",
    "staff_sales_id": 10,
    "counterparty_id": 1,
    "units_sold": 5000,
    "unit_price": 3.5,
    "currency_id": 1,
    "design_id": 101,
    "agreed_payment_date": "2024-12-05",
    "agreed_delivery_date": "2024-12-01",
    "agreed_delivery_location_id":20
  },
  {
    "sales_record_id": 2,
    "sales_order_id": 2,
    "created_date": "2024-01-13",
    "created_time": "12:05:00",
    "last_updated_date": "2024-04-13",
    "last_updated_time": "12:05:00",
    "staff_sales_id": 12,
    "counterparty_id": 2,
    "units_sold": 15000,
    "unit_price": 2.75,
    "currency_id": 2,
    "design_id": 102,
    "agreed_payment_date": "2024-12-15",
    "agreed_delivery_date": "2024-12-10",
    "agreed_delivery_location_id":21
  }
]
def test_to_fact_sales_order_happy_case():
    output = transform_to_fact_sales_order(sales_sample_data)
    expected_json_output = json.dumps(expected_output, separators=(',',':'))

    assert output == expected_json_output

def test_returns_error_if_sales_order_data_is_empty():
    with pytest.raises(Exception, match='Error, sales_order_data is empty'):
        transform_to_fact_sales_order([])