import pandas as pd

'''I need to access the data from counterparty, address'''
''' counterparty_id --> counterparty_id
    counterparty_legal_name --> counter_legal_name
    counter_party_legal_address_id references the address table
    and will need the following columns from the address table
    address_line_1 --> counterparty_legal_address_line_1
    address_line_2 --> counterparty_legal_address_line_2
    district --> counterparty_legal_district
    city --> counterparty_legal_city
    country --> counterparty_legal_country
    phone --> counterparty_phone_number'''

def transform_to_dim_counterparty(counterparty_data, address_data):
    
    '''Counterparty_data and address_data should be a list of dictionaries 
    in json format so convert the tables needed to a dataframe and then 
    merge on address_id = legal_address_id'''
    if not counterparty_data:
        raise Exception('Error, counterparty_data is empty')
    if not address_data:
        raise Exception('Error, address_data is empty')
    counterparty_df = pd.DataFrame(counterparty_data)
    address_df = pd.DataFrame(address_data)

    merged_df = counterparty_df.merge(address_df, 
                                      left_on='legal_address_id', 
                                      right_on='address_id')
    
    '''Select only the columns needed from each table and rename them
    if needed'''
    result_df = merged_df[[
    "counterparty_id",
    "counterparty_legal_name",
    "address_line_1",
    "address_line_2",
    "district",
    "city",
    "country",
    "phone"
    ]].rename(columns={
    "address_line_1": "counterparty_legal_address_line_1",
    "address_line_2": "counterparty_legal_address_line_2",
    "district": "counterparty_legal_district",
    "city": "counterparty_legal_city",
    "country": "counterparty_legal_country",
    "phone": "counterparty_legal_phone_number"
    })
    
    '''Convert this dataframe back to json format
    orient='records' gives it to us as a list of dictionaries'''
    result_json = result_df.to_json(orient='records')
    
    return result_json
