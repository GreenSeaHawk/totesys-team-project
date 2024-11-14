import json
import pytest
from src.transform_to_dim_staff import transform_to_dim_staff

staff_sample_data = [
    {
      "staff_id": 1,
      "first_name": "John",
      "last_name": "Doe",
      "department_id": 1,
      "email_address": "john.doe@example.com",
      "created_at": "2024-01-10T08:30:00Z",
      "last_updated": "2024-01-10T08:30:00Z"
    },
    {
      "staff_id": 2,
      "first_name": "Jane",
      "last_name": "Smith",
      "department_id": 2,
      "email_address": "jane.smith@example.com",
      "created_at": "2024-01-11T09:00:00Z",
      "last_updated": "2024-01-11T09:00:00Z"
    },
    {
      "staff_id": 3,
      "first_name": "Emily",
      "last_name": "Davis",
      "department_id": 2,
      "email_address": "emily.davis@example.com",
      "created_at": "2024-01-12T10:30:00Z",
      "last_updated": "2024-01-12T10:30:00Z"
    }]

department_sample_data = [
    {
      "department_id": 1,
      "department_name": "Engineering",
      "location": "New York",
      "manager": "Alice Johnson",
      "created_at": "2024-01-01T08:00:00Z",
      "last_updated": "2024-01-01T08:00:00Z"
    },
    {
      "department_id": 2,
      "department_name": "Human Resources",
      "location": None,
      "manager": None,
      "created_at": "2024-01-03T10:00:00Z",
      "last_updated": "2024-01-03T10:00:00Z"
    }]

expected_output = [
  {
    "staff_id": 1,
    "first_name": "John",
    "last_name": "Doe",
    "department_name": "Engineering",
    "location": "New York",
    "email_address": "john.doe@example.com"
  },
  {
    "staff_id": 2,
    "first_name": "Jane",
    "last_name": "Smith",
    "department_name": "Human Resources",
    "location": None,
    "email_address": "jane.smith@example.com"
  },
  {
    "staff_id": 3,
    "first_name": "Emily",
    "last_name": "Davis",
    "department_name": "Human Resources",
    "location": None,
    "email_address": "emily.davis@example.com"
  }
]

def test_dim_counterparty_happy_case():
    '''Normalise the data so that it doesn't give an assertion
    error due to whitespace or different quotes'''
    output = transform_to_dim_staff(
        staff_sample_data, 
        department_sample_data
        )
    expected_json_output = json.dumps(expected_output, separators=(',',':'))

    assert output == expected_json_output

def test_returns_error_if_staff_data_is_empty():
    with pytest.raises(Exception, match='Error, staff_data is empty'):
        transform_to_dim_staff([], department_sample_data)

def test_returns_error_if_department_data_is_empty():
    with pytest.raises(Exception, match='Error, department_data is empty'):
        transform_to_dim_staff(staff_sample_data, [])