import json


def transform_to_dim_design(design_data):
    """Raise error if data is empty"""
    if not design_data:
        raise Exception("Error, design_data is empty")
    """Simply have to convert it to a table with different
    names so have done this below"""
    dim_design_entries = []
    for design in design_data:
        temp_dict = {
            "design_id": design["design_id"],
            "design_name": design["design_name"],
            "file_location": design["file_location"],
            "file_name": design["file_name"],
        }
        dim_design_entries.append(temp_dict)
    """Have used separators to keep the format the same as the other tables
    where .to_json from pandas outputs without whitespace"""
    return json.dumps(dim_design_entries, separators=(",", ":"))
