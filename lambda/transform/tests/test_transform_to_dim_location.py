import json
import pytest
from src.transform_to_dim_location import transform_to_dim_location

address_data_sample = [
    {
        "address_id": 1,
        "address_line_1": "123 Elm Street",
        "address_line_2": "Suite 4B",
        "district": "Downtown",
        "city": "Metropolis",
        "postal_code": "12345",
        "country": "USA",
        "phone": "+1-555-1234",
    },
    {
        "address_id": 2,
        "address_line_1": "456 Maple Avenue",
        "address_line_2": None,
        "district": "Uptown",
        "city": "Springfield",
        "postal_code": "67890",
        "country": "USA",
        "phone": "+1-555-5678",
    },
]

expected_output = [
    {
        "location_id": 1,
        "address_line_1": "123 Elm Street",
        "address_line_2": "Suite 4B",
        "district": "Downtown",
        "city": "Metropolis",
        "postal_code": "12345",
        "country": "USA",
        "phone": "+1-555-1234",
    },
    {
        "location_id": 2,
        "address_line_1": "456 Maple Avenue",
        "address_line_2": None,
        "district": "Uptown",
        "city": "Springfield",
        "postal_code": "67890",
        "country": "USA",
        "phone": "+1-555-5678",
    },
]


def test_dim_location_happy_case():
    output = transform_to_dim_location(address_data_sample)
    expected_json_output = json.dumps(expected_output, separators=(",", ":"))

    assert output == expected_json_output


def test_returns_error_if_address_data_is_empty():
    with pytest.raises(Exception, match="Error, address_data is empty"):
        transform_to_dim_location([])
