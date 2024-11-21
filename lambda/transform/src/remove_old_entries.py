# This function takes all the address (or department) data which is a list of dictionaries. Filter out the records for address/department, having the same address/department id, remove the old entries based on the last update.


def filter_latest_data(data, key):
    if not data:
        raise ValueError("The data is empty or invalid.")
    # create a dictionary to track the latest record of each address_id
    latest_data = {}

    for record in data:
        id = record.get(key)

        if id is None:
            continue

        # update the dictionary with the latest record
        if (
            id not in latest_data
            or latest_data[id]["last_updated"] < record["last_updated"]
        ):
            latest_data[id] = record

    return list(latest_data.values())
