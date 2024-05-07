import pendulum
import dlt
from dlt.sources import incremental
from sqlalchemy import create_engine, text
from sql_database import sql_database  # Assuming this is a custom module to establish database connection
import subprocess

def load_database_table(credential, db_name, incremental_col):
    source = sql_database(credentials=credential, schema='stg')
    for table_name in source.resources.keys():
        incremental_source = incremental(incremental_col, initial_value=pendulum.datetime(1999, 1, 1, 0, 0, 0))
        source.resources[table_name].apply_hints(incremental=incremental_source)
        pipeline = dlt.pipeline(
            pipeline_name=db_name, destination="postgres", dataset_name="Src_load_inc", progress="log"
        )
        
        #Drop the existing dataset
        #pipeline.drop()
        
        info = pipeline.run(source.with_resources(table_name), table_name=f"{table_name}_{db_name}", write_disposition="append")
        print(info)
        
        # Access the last_trace attribute from the pipeline
        last_trace = pipeline.last_trace

        # Get the row counts
        row_counts = last_trace.last_normalize_info.row_counts
        row_counts.pop('_dlt_pipeline_state', None)
        print("Row counts:", row_counts)
        #print(row_counts)
        
        # Check if row_counts is empty
        if not row_counts:
            row_counts = 0
        else:
        # Extract the value from the dictionary if it's not empty
            table_name1=f"{table_name}_{db_name}"
            row_counts = row_counts[table_name1] 
            print(row_counts)

        engine1 = create_engine("postgresql://loader:Rahul_1234@localhost:5432/dlt_data")
        with engine1.connect() as conn:
            # Check if the table name exists
            query = text(f"SELECT COUNT(*) FROM dwh.multisource_log WHERE table_name = '{table_name}_{db_name}'")
            result = conn.execute(query)
            count = result.scalar()
            print(count)

            # If the table name exists, update the row count
            if count > 0:
                query = text(f"UPDATE dwh.multisource_log SET row_processed = {row_counts}, modified_date = CURRENT_TIMESTAMP WHERE table_name = '{table_name}_{db_name}'")
            # If the table name does not exist, insert a new row
            else:
                query = text(f"INSERT INTO dwh.multisource_log (table_name, row_processed) VALUES ('{table_name}_{db_name}', {row_counts})")
            print(query)
            conn.execute(query)
            conn.commit()

if __name__ == "__main__":     
    engine = create_engine("postgresql://loader:Rahul_1234@localhost:5432/dlt_data")
    with engine.connect() as conn:
        query = text("select db_name, db_schema, credential, incremental_col from dwh.source_cred")
        result = conn.execute(query)
        rows = result.fetchall()
        print(rows)
    for row in rows:
        db_name, db_schema, credential, incremental_col = row
        print(row)
        load_database_table(credential, db_name, incremental_col)
