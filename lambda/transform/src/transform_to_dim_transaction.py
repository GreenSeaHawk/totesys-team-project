import json

def transform_to_dim_transaction(transaction_data):
    '''Raise error if data is empty'''
    if not transaction_data:
        raise Exception('Error, transaction_data is empty')
    '''Simply have to convert it to a table with different
    names so have done this below'''
    dim_transaction_entries = []
    for transaction in transaction_data:
        temp_dict = {
            "transaction_id": transaction["transaction_id"],
            "transaction_name": transaction["transaction_name"]
        }
        dim_transaction_entries.append(temp_dict)
    '''Have used separators to keep the format the same as the other tables
    where .to_json from pandas outputs without whitespace'''
    return json.dumps(dim_transaction_entries, separators=(',',':'))