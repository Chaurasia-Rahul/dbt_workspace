import humanize
import dlt
from sql_database import sql_database

def load_entire_database() -> None:
    """Use the sql_database source to completely load all tables in a database"""
    pipeline = dlt.pipeline(
        pipeline_name="sakila_wh", destination='postgres', dataset_name="test_load"
    )

    # By default the sql_database source reflects all tables in the schema
    source = sql_database(schema='stg')

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    print(info)

if __name__ == "__main__":
    load_entire_database()
    
    
    
