import pandas as pd
from iso4217 import Currency # Need to add to requirements file

'''Need to access data from currency and use iso4217 to
convert currency_code to currency_name
example of iso4217: print(Currency('GBP').currency_name) # 'Pound Sterling'
currency_id --> currency_id
currency_code --> currency_code
currency_code --> currency_name'''

def transform_to_dim_currency(currency_data):
    '''Raise error if data is empty'''
    if not currency_data:
        raise Exception('Error, currency_data is empty')
    '''Convert data to a dataframe'''
    currency_df = pd.DataFrame(currency_data)
    '''Simple helper function to convert currency_code to currency_name'''
    def get_currency_name(currency_code):
        return Currency(currency_code).currency_name
    '''Add currency_name as a column'''    
    currency_df["currency_name"] = currency_df["currency_code"].apply(get_currency_name)
    '''Remove unnecessary columns'''
    result_df = currency_df[[
        "currency_id",
        "currency_code",
        "currency_name"
    ]]
    
    result_json = result_df.to_json(orient='records')
    return result_json





