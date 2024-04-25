#multisource_postgres_bytables.py

import pendulum
import dlt
from dlt.sources import incremental
from sqlalchemy import create_engine, text
from sql_database import sql_database

def load_database_table(credential, db_name,incremental_col):
    source = sql_database(credentials=credential, schema='stg')
    for table_name in source.resources.keys():
        incremental_source = incremental(incremental_col, initial_value=pendulum.datetime(1999, 1, 1, 0, 0, 0))
        source.resources[table_name].apply_hints(incremental=incremental_source)
        pipeline = dlt.pipeline(
            pipeline_name=db_name, destination="postgres", dataset_name="Src_load_inc"
        )
        info = pipeline.run(source.with_resources(table_name), table_name=f"{table_name}_{db_name}", write_disposition="merge")
        print(info)

        
if __name__ == "__main__":     
    engine = create_engine("postgresql://loader:Rahul_1234@localhost:5432/dlt_data")
    with engine.connect() as conn:
        query = text("select db_name, db_schema,credential,incremental_col from dwh.source_cred")
        result = conn.execute(query)
        rows = result.fetchall()
        print(rows)
    for row in rows:
        db_name, db_schema, credential,incremental_col = row
        print(row)
        load_database_table(credential, db_name,incremental_col)