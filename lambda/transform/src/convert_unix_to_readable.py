from datetime import datetime


# Function to convert Unix timestamp to human-readable format
def convert_unix_to_readable(unix_timestamp):
    # Convert milliseconds to seconds
    timestamp_in_seconds = unix_timestamp / 1000
    # Format the timestamp as a string
    readable_date = datetime.fromtimestamp(timestamp_in_seconds).strftime('%Y-%m-%d %H:%M:%S')
    return readable_date

