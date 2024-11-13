import json
import pytest
from src.transform_to_dim_date import transform_to_dim_date

'''Set up sample data'''

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
        "created_at": "2024-11-13T12:05:00Z",
        "last_updated": "2024-11-13T12:05:00Z",
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
    }
]

def test_dim_date_happy_case():
    output = transform_to_dim_date(sales_sample_data)
    expected_json_output = json.dumps(expected_output, separators=(',',':'))

    assert output == expected_json_output

def test_returns_error_if_sales_order_data_is_empty():
    with pytest.raises(Exception, match='Error, sales_order_data is empty'):
        transform_to_dim_date([])