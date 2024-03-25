import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from sql_database import sql_database, sql_table, Table


def load_select_tables_from_database() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="sakila_wh", destination="postgres", dataset_name="mulitsource_data"
    )

    # Credentials for the sample database.
    # Note: It is recommended to configure credentials in `.dlt/secrets.toml` under `sources.sql_database.credentials`
    source = sql_database()
    
    schema = "stg"
    # Configure the source to load a few select tables incrementally
    source_1 = sql_database(schema=schema).with_resources("actor","address")


    info = pipeline.run(source_1, write_disposition="replace")
    print(info)

    
if __name__ == "__main__":
    load_select_tables_from_database()