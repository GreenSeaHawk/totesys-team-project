import json

def transform_to_dim_payment_type(payment_type_data):
    '''Raise error if data is empty'''
    if not payment_type_data:
        raise Exception('Error, payment_type_data is empty')
    '''Simply have to convert it to a table with different
    names so have done this below'''
    dim_location_entries = []
    for payment_type in payment_type_data:
        temp_dict = {
            "payment_type_id": payment_type["payment_type_id"],
            "payment_type_name": payment_type["payment_type_name"]
        }
        dim_location_entries.append(temp_dict)
    '''Have used separators to keep the format the same as the other tables
    where .to_json from pandas outputs without whitespace'''
    return json.dumps(dim_location_entries, separators=(',',':'))