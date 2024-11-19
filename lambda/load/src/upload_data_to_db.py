def upload_dataframe_to_database(data, conn):
    pass



engine = create_engine(
    f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
)


def insert_data_to_postgres(df, table_name, engine):
    """Insert a DataFrame into a PostgreSQL table."""
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Inserted {len(df)} rows into {table_name}.")