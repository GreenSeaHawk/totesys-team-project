from datetime import datetime
import boto3
from sqlalchemy.dialects.postgresql import insert


# Helper function for insert data to postgres
# Tested that its called correctly
# Tested on test RDS which is no longer live
def postgres_upsert(table, conn, keys, data_iter):

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)


def insert_data_to_postgres(df, table_name, engine):
    """Insert a DataFrame into a PostgreSQL table."""
    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method=postgres_upsert,
    )
    print(f"Inserted {len(df)} rows into {table_name}.")


def update_last_ran_s3(bucket_name, Key):
    """after processing update the last_ran file
    in s3 with the current timestamp"""
    s3_client = boto3.client("s3")
    current_time = datetime.now().isoformat()
    s3_client.put_object(Bucket=bucket_name, Key=Key, Body=current_time)
