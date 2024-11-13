import pandas as pd

address_data_sample = [
    {"address_id": 1, "address_line_1": "123 Elm Street", "address_line_2": "Suite 4B", "district": "Downtown", "city": "Metropolis", "postal_code": "12345", "country": "USA", "phone": "+1-555-1234"},
    {"address_id": 2, "address_line_1": "456 Maple Avenue", "address_line_2": None, "district": "Uptown", "city": "Springfield", "postal_code": "67890", "country": "USA", "phone": "+1-555-5678"},
    {"address_id": 3, "address_line_1": "789 Oak Drive", "address_line_2": None, "district": None, "city": "Gotham", "postal_code": "54321", "country": "USA", "phone": "+1-555-9101"},
    {"address_id": 4, "address_line_1": "321 Pine Road", "address_line_2": "Floor 3", "district": "Midtown", "city": "Central City", "postal_code": "13579", "country": "USA", "phone": "+1-555-1112"},
    {"address_id": 5, "address_line_1": "654 Cedar Lane", "address_line_2": None, "district": "Westside", "city": "Star City", "postal_code": "24680", "country": "USA", "phone": "+1-555-1314"}
]
counterparty_data_sample = [
    {"counterparty_id": 1, "counterparty_legal_name": "Acme Corp", "legal_address_id": 1, "commercial_contact": "John Doe", "delivery_contact": "Jane Smith"},
    {"counterparty_id": 2, "counterparty_legal_name": "Globex Inc.", "legal_address_id": 2, "commercial_contact": "Emily Davis", "delivery_contact": None},
    {"counterparty_id": 3, "counterparty_legal_name": "Initech Ltd.", "legal_address_id": 3, "commercial_contact": None, "delivery_contact": "Michael Bolton"},
    {"counterparty_id": 4, "counterparty_legal_name": "Stark Industries", "legal_address_id": 4, "commercial_contact": "Tony Stark", "delivery_contact": "Pepper Potts"},
    {"counterparty_id": 5, "counterparty_legal_name": "Wayne Enterprises", "legal_address_id": 5, "commercial_contact": "Bruce Wayne", "delivery_contact": None}
]
expected_json_output = [
    {
        "counterparty_id": 1,
        "counterparty_legal_name": "Acme Corp",
        "counterparty_legal_address_line_1": "123 Elm Street",
        "counterparty_legal_address_line_2": "Suite 4B",
        "counterparty_legal_district": "Downtown",
        "counterparty_legal_city": "Metropolis",
        "counterparty_legal_country": "USA",
        "counterparty_legal_phone_number": "+1-555-1234"
    },
    {
        "counterparty_id": 2,
        "counterparty_legal_name": "Globex Inc.",
        "counterparty_legal_address_line_1": "456 Maple Avenue",
        "counterparty_legal_address_line_2": None,
        "counterparty_legal_district": "Uptown",
        "counterparty_legal_city": "Springfield",
        "counterparty_legal_country": "USA",
        "counterparty_legal_phone_number": "+1-555-5678"
    },
    {
        "counterparty_id": 3,
        "counterparty_legal_name": "Initech Ltd.",
        "counterparty_legal_address_line_1": "789 Oak Drive",
        "counterparty_legal_address_line_2": None,
        "counterparty_legal_district": None,
        "counterparty_legal_city": "Gotham",
        "counterparty_legal_country": "USA",
        "counterparty_legal_phone_number": "+1-555-9101"
    },
    {
        "counterparty_id": 4,
        "counterparty_legal_name": "Stark Industries",
        "counterparty_legal_address_line_1": "321 Pine Road",
        "counterparty_legal_address_line_2": "Floor 3",
        "counterparty_legal_district": "Midtown",
        "counterparty_legal_city": "Central City",
        "counterparty_legal_country": "USA",
        "counterparty_legal_phone_number": "+1-555-1112"
    },
    {
        "counterparty_id": 5,
        "counterparty_legal_name": "Wayne Enterprises",
        "counterparty_legal_address_line_1": "654 Cedar Lane",
        "counterparty_legal_address_line_2": None,
        "counterparty_legal_district": "Westside",
        "counterparty_legal_city": "Star City",
        "counterparty_legal_country": "USA",
        "counterparty_legal_phone_number": "+1-555-1314"
    }
]

counterparty_df = pd.DataFrame(counterparty_data_sample)
address_df = pd.DataFrame(address_data_sample)

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
print(result_df)
print(result_json)