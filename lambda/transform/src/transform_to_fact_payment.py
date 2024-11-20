import json

'''Need data from payment
create a new serial --> payment_record_id (int)
payment_id --> payment_id (int)
created_at (timestamp) --> created_date (date: yyyy-mm-dd)
created_at (timestamp) --> created_time (time: NN:NN:NN)
last_updated (timestamp) --> last_updated_date (date: yyyy-mm-dd)
last_updated (timestamp) --> last_updated_time (time: NN:NN:NN)
transaction_id (int) --> transaction_id (int)
counterparty_id (int) --> counterparty_id (int)
paymount_amount (numeric/float) --> paymount_amount (numeric/float)
currency_id (int) --> currency_id (int)
payment_type_id (int) --> payment_type_id (int)
paid (bool) --> paid (bool)
payment_date (varchar in form yyyy-mm-dd) --> payment_date (date: yyyy-mm-dd)
'''

def transform_to_fact_payment(payment_data):
    '''Raise error if data is empty'''
    if not payment_data:
        raise Exception('Error, payment_data is empty')

    '''Convert all the entries and use a count make the new
    sales_record_id'''
    payment_entries = []
    count = 1
    
    for payment in payment_data:
        temp_dict = {
            "payment_record_id": count,
            "payment_id": payment["payment_id"],
            "created_date": payment["created_at"][:10],
            "created_time": payment["created_at"][11:19],
            "last_updated_date": payment["last_updated"][:10],
            "last_updated_time": payment["last_updated"][11:19],
            "transaction_id": payment["transaction_id"],
            "counterparty_id": payment["counterparty_id"],
            "payment_amount": payment["payment_amount"],
            "currency_id": payment["currency_id"],
            "payment_type_id": payment["payment_type_id"],
            "paid": payment["paid"],
            "payment_date": payment["payment_date"]
        }
        payment_entries.append(temp_dict)
        count += 1

    return json.dumps(payment_entries, separators=(',',':'))