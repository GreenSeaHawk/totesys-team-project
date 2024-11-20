import json
import pytest
from src.transform_to_dim_date import transform_to_dim_date

'''Set up sample data'''

sales_sample_data = [
    {
        "sales_order_id": 1,
        "created_at": 1731947889824,
        "last_updated": 1731947889824,
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

payment_sample_data = [
  {
    "payment_id": 1,
    "transaction_id": 101,
    "counterparty_id": 5001,
    "payment_amount": 25000.75,
    "currency_id": 1,
    "payment_type_id": 2,
    "paid": True,
    "payment_date": "2024-06-15",
    "company_ac_number": 12345678,
    "counterparty_ac_number": 87654321
  },
  {
    "payment_id": 2,
    "transaction_id": 102,
    "counterparty_id": 5002,
    "payment_amount": 150000.50,
    "currency_id": 2,
    "payment_type_id": 3,
    "paid": False,
    "payment_date": "2024-07-10",
    "company_ac_number": 23456789,
    "counterparty_ac_number": 98765432
  }
]

expected_output = [
  {
    "date_id": "2024-11-13",
    "year": 2024,
    "month": 11,
    "day": 13,
    "day_of_week": 3,
    "day_name": "Wednesday",
    "month_name": "November",
    "quarter": 4
  },
  {
    "date_id": "2024-12-01",
    "year": 2024,
    "month": 12,
    "day": 1,
    "day_of_week": 7,
    "day_name": "Sunday",
    "month_name": "December",
    "quarter": 4
  },
  {
    "date_id": "2024-12-05",
    "year": 2024,
    "month": 12,
    "day": 5,
    "day_of_week": 4,
    "day_name": "Thursday",
    "month_name": "December",
    "quarter": 4
  },
  {
    "date_id": "2024-01-13",
    "year": 2024,
    "month": 1,
    "day": 13,
    "day_of_week": 6,
    "day_name": "Saturday",
    "month_name": "January",
    "quarter": 1
  },
  {
    "date_id": "2024-04-13",
    "year": 2024,
    "month": 4,
    "day": 13,
    "day_of_week": 6,
    "day_name": "Saturday",
    "month_name": "April",
    "quarter": 2
  },
  {
    "date_id": "2024-12-10",
    "year": 2024,
    "month": 12,
    "day": 10,
    "day_of_week": 2,
    "day_name": "Tuesday",
    "month_name": "December",
    "quarter": 4
  },
  {
    "date_id": "2024-12-15",
    "year": 2024,
    "month": 12,
    "day": 15,
    "day_of_week": 7,
    "day_name": "Sunday",
    "month_name": "December",
    "quarter": 4
  },
  {
    "date_id": "2024-06-15",
    "year": 2024,
    "month": 6,
    "day": 15,
    "day_of_week": 6,
    "day_name": "Saturday",
    "month_name": "June",
    "quarter": 2
  },
  {
    "date_id": "2024-07-10",
    "year": 2024,
    "month": 7,
    "day": 10,
    "day_of_week": 3,
    "day_name": "Wednesday",
    "month_name": "July",
    "quarter": 3
  }
]

def test_dim_date_happy_case():
    output = transform_to_dim_date(sales_sample_data, payment_sample_data)
    expected_json_output = json.dumps(expected_output, separators=(',',':'))

    assert output == expected_json_output

def test_returns_error_if_all_data_empty():
    with pytest.raises(Exception, match='Error, sales_order_data and payment_data are empty'):
        transform_to_dim_date([], [])

def test_returns_error_if_date_data_in_wrong_format():
    bad_data_sales = [
    {
        "sales_order_id": 1,
        "created_at": "202-11-13T12:00:00Z",
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
    }]
    with pytest.raises(ValueError):
        transform_to_dim_date(bad_data_sales, [])