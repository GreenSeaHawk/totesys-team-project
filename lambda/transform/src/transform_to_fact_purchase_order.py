import json
import boto3
from src.convert_unix_to_readable import convert_unix_to_readable


"""Need data from purchase_order
create a new serial --> purchase_record_id (int)
purchase_order_id --> purchase_order_id (int)
created_at (timestamp) --> created_date (date: yyyy-mm-dd)
created_at (timestamp) --> created_time (time: NN:NN:NN)
last_updated (timestamp) --> last_updated_date (date: yyyy-mm-dd)
last_updated (timestamp) --> last_updated_time (time: NN:NN:NN)
staff_id (int) --> staff_id (int)
counterparty_id (int) --> counterparty_id (int)
item_code (varchar) --> item_code (varchar)
item_quantity (int) --> item_quantity (int)
item_unit_price (numeric/float) --> item_unit_price (numeric/float)
currency_id (int) --> currency_id (int)
agreed_delivery_date (varchar) --> agreed_delivery_date_date (date: yyyy-mm-dd)
agreed_payment_date (timestamp) --> agreed_payment_date_time (time: NN:NN:NN)
agreed_delivery_location_id (int) --> agreed_delivery_location_id (int)

"""


def transform_to_fact_purchase_order(
    purchase_order_data, Bucket="totesys-data-bucket-cimmeria"
):
    """Raise error if data is empty"""
    if not purchase_order_data:
        raise Exception("Error, purchase_order_data is empty")

    """Convert all the entries and use a count make the new
    purchase_record_id"""
    purchase_order_entries = []

    """Check if there has already been an id saved
    if not then start the count at 1"""
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(
            Bucket=Bucket, Key="fact-purchase-order-highest-id.txt"
        )
        count = int(response["Body"].read().decode("utf-8"))
    except:
        count = 1

    for purchase_order in purchase_order_data:
        temp_dict = {
            "purchase_record_id": count,
            "purchase_order_id": purchase_order["purchase_order_id"],
            "created_date": convert_unix_to_readable(
                purchase_order["created_at"]
            )[:10],
            "created_time": convert_unix_to_readable(
                purchase_order["created_at"]
            )[11:19],
            "last_updated_date": convert_unix_to_readable(
                purchase_order["last_updated"]
            )[:10],
            "last_updated_time": convert_unix_to_readable(
                purchase_order["last_updated"]
            )[11:19],
            "staff_id": purchase_order["staff_id"],
            "counterparty_id": purchase_order["counterparty_id"],
            "item_code": purchase_order["item_code"],
            "item_quantity": purchase_order["item_quantity"],
            "item_unit_price": purchase_order["item_unit_price"],
            "currency_id": purchase_order["currency_id"],
            "agreed_delivery_date": purchase_order["agreed_delivery_date"],
            "agreed_payment_date": purchase_order["agreed_payment_date"],
            "agreed_delivery_location_id": purchase_order[
                "agreed_delivery_location_id"
            ],
        }
        purchase_order_entries.append(temp_dict)
        count += 1

    """Save the count as the highest id"""
    count_string = str(count)
    s3.put_object(
        Bucket=Bucket,
        Key="fact-purchase-order-highest-id.txt",
        Body=count_string,
    )

    return json.dumps(purchase_order_entries, separators=(",", ":"))
