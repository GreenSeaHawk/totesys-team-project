import pytest
from datetime import datetime
from src.remove_old_entries import filter_latest_data


# empty input
# single record - return it as is
# duplcate recors = ensure to return only the latest one
# missing keys / invalid keys

SAMPLE_SINGLE_DATA = [{"address_id": 1, "last_updated": "2023-01-01"}]

SAMPLE_DUPLICATE_DATA = [
    {
        "address_id": 1,
        "address_line_1": "123 Elm Street",
        "address_line_2": "Suite 4B",
        "district": "Downtown",
        "city": "Metropolis",
        "postal_code": "12345",
        "country": "USA",
        "phone": "+1-555-1234",
        "created_at": 1731947889824,
        "last_updated": 1731947889824,
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
        "created_at": 1731947889824,
        "last_updated": 1731947889824,
    },
    {
        "address_id": 1,
        "address_line_1": "789 Oak Drive",
        "address_line_2": None,
        "district": None,
        "city": "Gotham",
        "postal_code": "54321",
        "country": "USA",
        "phone": "+1-555-9101",
        "created_at": 1667485249962,
        "last_updated": 1667485249962,
    },
]

SAMPLE_DEPARTMENT_DATA_WITHOUT_ID = [
    {
        "department_id": 1,
        "department_name": "Engineering",
        "location": "New York",
        "manager": "Alice Johnson",
        "created_at": 1704096000,
        "last_updated": 1704096000,
    },
    {
        "department_name": "Human Resources",
        "location": None,
        "manager": None,
        "created_at": 1704276000,
        "last_updated": 1704276000,
    },
    {
        "department_name": "Human Resources",
        "location": "Manchester",
        "manager": "Joe Smith",
        "created_at": 1712109600,
        "last_updated": 1704276000,
    },
]


def test_empty_input():
    with pytest.raises(ValueError, match="The data is empty or invalid."):
        filter_latest_data([], key="address_id")


def test_single_record():
    result = filter_latest_data(SAMPLE_SINGLE_DATA, key="address_id")
    assert result == SAMPLE_SINGLE_DATA


def test_duplicate_records():
    result = filter_latest_data(SAMPLE_DUPLICATE_DATA, key="address_id")

    expected_output = [
        {
            "address_id": 1,
            "address_line_1": "123 Elm Street",
            "address_line_2": "Suite 4B",
            "district": "Downtown",
            "city": "Metropolis",
            "postal_code": "12345",
            "country": "USA",
            "phone": "+1-555-1234",
            "created_at": 1731947889824,
            "last_updated": 1731947889824,
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
            "created_at": 1731947889824,
            "last_updated": 1731947889824,
        },
    ]

    assert sorted(result, key=lambda x: x["address_id"]) == sorted(
        expected_output, key=lambda x: x["address_id"]
    )


def test_missing_key():
    result = filter_latest_data(
        SAMPLE_DEPARTMENT_DATA_WITHOUT_ID, key="department_id"
    )
    expected_outcome = [
        {
            "department_id": 1,
            "department_name": "Engineering",
            "location": "New York",
            "manager": "Alice Johnson",
            "created_at": 1704096000,
            "last_updated": 1704096000,
        }
    ]
    assert result == expected_outcome
