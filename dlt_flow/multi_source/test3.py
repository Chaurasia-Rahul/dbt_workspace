import dlt
import pyodbc
from sql_database import sql_database

def load_select_tables_from_database(pipeline_name,schema, resources) -> None:
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name, destination="postgres", dataset_name="multisource_data"
    )
    source = sql_database(schema=schema).with_resources(*resources)
    info = pipeline.run(source, write_disposition="replace")
    print(info)

if __name__ == "__main__":
    load_select_tables_from_database("sakila_wh","stg", ["actor", "address", "category"])
    load_select_tables_from_database("sakila_wh","shd", ["dim_date", "dim_customer"])
    load_select_tables_from_database("adventure", "stg",["product", "productcategory", "productsubcategory"])