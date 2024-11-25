import json
import boto3
from convert_unix_to_readable import convert_unix_to_readable

"""Need data from payment
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
"""
from pprint import pprint

def transform_to_fact_payment(
    payment_data, Bucket="totesys-data-bucket-cimmeria"
):  
    """Raise error if data is empty"""
    if not payment_data:
        raise Exception("Error, payment_data is empty")

    """Convert all the entries and use a count make the new
    sales_record_id"""
    payment_entries = []

    """Check if there has already been an id saved
    if not then start the count at 1"""
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(
            Bucket=Bucket, Key="fact-payment-highest-id.txt"
        )
        count = int(response["Body"].read().decode("utf-8"))
    except:
        count = 1

    for payment in payment_data:
        temp_dict = {
            "payment_record_id": count,
            "payment_id": payment["payment_id"],
            "created_date": convert_unix_to_readable(payment["created_at"])[
                :10
            ],
            "created_time": convert_unix_to_readable(payment["created_at"])[
                11:19
            ],
            "last_updated_date": convert_unix_to_readable(
                payment["last_updated"]
            )[:10],
            "last_updated_time": convert_unix_to_readable(
                payment["last_updated"]
            )[11:19],
            "transaction_id": payment["transaction_id"],
            "counterparty_id": payment["counterparty_id"],
            "payment_amount": payment["payment_amount"],
            "currency_id": payment["currency_id"],
            "payment_type_id": payment["payment_type_id"],
            "paid": payment["paid"],
            "payment_date": payment["payment_date"],
        }
        payment_entries.append(temp_dict)
        count += 1

    """Save the count as the highest id"""
    count_string = str(count)
    s3.put_object(
        Bucket=Bucket, Key="fact-payment-highest-id.txt", Body=count_string
    )

    return json.dumps(payment_entries, separators=(",", ":"))
