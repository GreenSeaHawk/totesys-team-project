import json
import pandas as pd

"""Need sales_data and department_data and they merge on department_id
staff_id --> staff_id
first_name --> first_name
last_name --> last_name
department_id --> department_name
department_id --> location
email_address --> email_address
"""


def transform_to_dim_staff(staff_data, department_data):
    """Counterparty_data and address_data should be a list of dictionaries
    in json format so convert the tables needed to a dataframe and then
    merge on address_id = legal_address_id"""
    if not staff_data:
        raise Exception("Error, staff_data is empty")
    if not department_data:
        raise Exception("Error, department_data is empty")
    staff_df = pd.DataFrame(staff_data)
    department_df = pd.DataFrame(department_data)

    merged_df = staff_df.merge(
        department_df, left_on="department_id", right_on="department_id"
    )

    """Select only the columns needed from each table and rename them
    if needed"""
    result_df = merged_df[
        [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]
    ]

    """Convert this dataframe back to json format
    orient='records' gives it to us as a list of dictionaries"""
    result_json = result_df.to_json(orient="records")

    return result_json
