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
    # Read parameters from the text file
    with open("parameters.txt", "r") as file:
        lines = file.readlines()
        for line in lines:
            params = line.strip().split()
            print(params)
            pipeline_name, schema, resources = params[0], params[1], params[2:]
            load_select_tables_from_database(pipeline_name, schema, resources)