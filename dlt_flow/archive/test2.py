import dlt
from sql_database import sql_database

def load_select_tables_from_multiple_databases() -> None:
    # Load data from the sakila_wh database
    pipeline = dlt.pipeline(
        pipeline_name="sakila_wh", destination="postgres", dataset_name="multisource_data"
    )
    source = sql_database(schema="stg").with_resources("actor", "address", "category")
    info1 = pipeline.run(source, write_disposition="replace")
    print(info1)

    # Load data from the adventure database
    pipeline = dlt.pipeline(
        pipeline_name="adventure", destination="postgres", dataset_name="multisource_data"
    )
    source = sql_database(schema="stg").with_resources("product", "productcategory", "productsubcategory")
    info2 = pipeline.run(source, write_disposition="replace")
    print(info2)

if __name__ == "__main__":
    load_select_tables_from_multiple_databases()