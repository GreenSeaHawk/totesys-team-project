import json
import pytest
from src.transform_to_dim_design import transform_to_dim_design

design_sample_data = [
  {
    "design_id": 1,
    "created_at": "2024-01-15T08:00:00Z",
    "last_updated": "2024-01-15T08:00:00Z",
    "design_name": "Logo Design",
    "file_location": "/designs/logos/",
    "file_name": "logo_v1.png"
  },
  {
    "design_id": 2,
    "created_at": "2024-01-16T09:30:00Z",
    "last_updated": "2024-01-16T09:30:00Z",
    "design_name": "Business Card",
    "file_location": "/designs/business_cards/",
    "file_name": "business_card_v2.pdf"
  },
  {
    "design_id": 3,
    "created_at": "2024-01-17T10:45:00Z",
    "last_updated": "2024-01-17T10:45:00Z",
    "design_name": "Website Mockup",
    "file_location": "/designs/web_mockups/",
    "file_name": "mockup_v3.psd"
  }
]

expected_output = [
  {
    "design_id": 1,
    "design_name": "Logo Design",
    "file_location": "/designs/logos/",
    "file_name": "logo_v1.png"
  },
  {
    "design_id": 2,
    "design_name": "Business Card",
    "file_location": "/designs/business_cards/",
    "file_name": "business_card_v2.pdf"
  },
  {
    "design_id": 3,
    "design_name": "Website Mockup",
    "file_location": "/designs/web_mockups/",
    "file_name": "mockup_v3.psd"
  }
]

def test_dim_location_happy_case():
    output = transform_to_dim_design(design_sample_data)
    expected_json_output = json.dumps(expected_output, separators=(',',':'))

    assert output == expected_json_output

def test_returns_error_if_sales_order_data_is_empty():
    with pytest.raises(Exception, match='Error, design_data is empty'):
        transform_to_dim_design([])