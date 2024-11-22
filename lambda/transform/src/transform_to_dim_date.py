import json
from datetime import datetime
from convert_unix_to_readable import convert_unix_to_readable

"""Need data from sales_order table
When the sales_order table is converted to fact_sales_order the
below conversions will happen. For each of the 4 dates there will be a 
dim_date table where the id is date_id (date: yyyy-mm-dd) some of these dates
might coincide and therefore share a dim_date table. So will need to
convert each of created_at, last_updated, agreed_delivery_date and
agreed_payment_date to a date in the format yyyy-mm-dd and then create
a table for each of these.

Also need payment_data as fact_payment references the dim_date table.

created_at (timestamp) --> created_date (date: yyyy-mm-dd)
last_updated (timestamp) --> last_updated_date (date: yyyy-mm-dd)
agreed_delivery_date (varchar in form yyyy-mm-dd) 
--> agreed_delivery_date (date: yyyy-mm-dd)
agreed_payment_date (varchar in form yyyy-mm-dd) 
--> agreed_payment_date (date: yyyy-mm-dd)"""


def transform_to_dim_date(sales_order_data, payment_data):
    """Have re-written to only raise an error if both of
    the datasets are empty"""
    if not sales_order_data and not payment_data:
        raise Exception("Error, sales_order_data and payment_data are empty")

    """Create list of dates from each of the 4 columns of
    sales_order_data"""
    list_of_dates = []
    for sale in sales_order_data:
        list_of_dates.append(convert_unix_to_readable(sale["created_at"])[:10])
        list_of_dates.append(
            convert_unix_to_readable(sale["last_updated"])[:10]
        )
        list_of_dates.append(sale["agreed_delivery_date"])
        list_of_dates.append(sale["agreed_payment_date"])
    """Add the dates from payment_data"""
    for payment in payment_data:
        list_of_dates.append(payment["payment_date"])

    """Remove duplicates"""
    list_of_unique_dates = list(dict.fromkeys(list_of_dates))

    dim_date_entries = []
    days_of_week = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    months_of_year = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    for date_str in list_of_unique_dates:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        entry = {
            "date_id": date_str,
            "year": date_obj.year,
            "month": date_obj.month,
            "day": date_obj.day,
            "day_of_week": date_obj.isoweekday(),
            "day_name": days_of_week[date_obj.weekday()],
            "month_name": months_of_year[date_obj.month - 1],
            "quarter": (date_obj.month - 1) // 3 + 1,
        }
        dim_date_entries.append(entry)
    """Have used separators to keep the format the same as the other tables
    where .to_json from pandas outputs without whitespace"""
    return json.dumps(dim_date_entries, separators=(",", ":"))
