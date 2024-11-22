import json
import boto3
from convert_unix_to_readable import convert_unix_to_readable

"""Need data from sales_order, 
create a new serial --> sales_record_id (int)
sales_order_id --> sales_order_id (int)
created_at (timestamp) --> created_date (date: yyyy-mm-dd)
created_at (timestamp) --> created_time (time: NN:NN:NN)
last_updated (timestamp) --> last_updated_date (date: yyyy-mm-dd)
last_updated (timestamp) --> last_updated_time (time: NN:NN:NN)
staff_id (int) --> staff_sales_id (int)
counterparty_id (int) --> counterparty_id (int)
units_sold (int) --> units_sold (int)
unit_price (numeric/float) --> unit_price (numeric/float)
currency_id (int) --> currency_id (int)
design_id (int) --> design_id (int)
agreed_payment_date (varchar in form yyyy-mm-dd) 
--> agreed_payment_date (date: yyyy-mm-dd)
agreed_delivery_date (varchar in form yyyy-mm-dd) 
--> agreed_delivery_date (date: yyyy-mm-dd)
agreed_delivery_location_id (int) --> agreed_delivery_location_id (int)
"""


def transform_to_fact_sales_order(
    sales_order_data, Bucket="totesys-data-bucket-cimmeria"
):
    """Raise error if data is empty"""
    if not sales_order_data:
        raise Exception("Error, sales_order_data is empty")

    """Convert all the entries and use a count make the new
    sales_record_id"""
    fact_sales_entries = []

    """Check if there has already been an id saved
    if not then start the count at 1"""
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(
            Bucket=Bucket, Key="fact-sales-order-highest-id.txt"
        )
        count = int(response["Body"].read().decode("utf-8"))
    except:
        count = 1

    for sales in sales_order_data:
        temp_dict = {
            "sales_record_id": count,
            "sales_order_id": sales["sales_order_id"],
            "created_date": convert_unix_to_readable(sales["created_at"])[:10],
            "created_time": convert_unix_to_readable(sales["created_at"])[
                11:19
            ],
            "last_updated_date": convert_unix_to_readable(
                sales["last_updated"]
            )[:10],
            "last_updated_time": convert_unix_to_readable(
                sales["last_updated"]
            )[11:19],
            "staff_sales_id": sales["staff_id"],
            "counterparty_id": sales["counterparty_id"],
            "units_sold": sales["units_sold"],
            "unit_price": sales["unit_price"],
            "currency_id": sales["currency_id"],
            "design_id": sales["design_id"],
            "agreed_payment_date": sales["agreed_payment_date"],
            "agreed_delivery_date": sales["agreed_delivery_date"],
            "agreed_delivery_location_id": sales[
                "agreed_delivery_location_id"
            ],
        }
        fact_sales_entries.append(temp_dict)
        count += 1

    """Save the count as the highest id"""
    count_string = str(count)
    s3.put_object(
        Bucket=Bucket, Key="fact-sales-order-highest-id.txt", Body=count_string
    )

    return json.dumps(fact_sales_entries, separators=(",", ":"))
