import json

def transform_to_dim_location(address_data):
    '''Raise error if data is empty'''
    if not address_data:
        raise Exception('Error, address_data is empty')
    dim_location_entries = []
    for address in address_data:
        temp_dict = {
            "location_id": address["address_id"],
            "address_line_1": address["address_line_1"],
            "address_line_2": address["address_line_2"],
            "district": address["district"],
            "city": address["address_id"],
            "postal_code": address["postal_code"],
            "country": address["country"],
            "phone": address["phone"]
        }
        dim_location_entries.append(temp_dict)
    
    return json.dumps(dim_location_entries, separators=(',',':'))