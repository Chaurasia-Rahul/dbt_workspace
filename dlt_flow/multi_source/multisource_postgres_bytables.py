import logging
import pendulum
import dlt
from dlt.sources import incremental
from sqlalchemy import create_engine, text
from sql_database import sql_database  
import subprocess
import db_credentials

# Configure logging
log_file_path = db_credentials.log_file_path
logging.basicConfig(filename=log_file_path, filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_database_table(credential, db_name, incremental_col, full_refresh):
    try:
        # Record the start time
        start_time = pendulum.now()

        source = sql_database(credentials=credential, schema='stg')
        for table_name in source.resources.keys():
            incremental_source = incremental(incremental_col, initial_value=pendulum.datetime(1999, 1, 1, 0, 0, 0))
            source.resources[table_name].apply_hints(incremental=incremental_source)
            pipeline = dlt.pipeline(
                pipeline_name=db_name, destination="postgres", dataset_name="Src_load_inc"#, progress="log"
            )
            
            # Set write_disposition based on full_refresh
            if full_refresh:
                write_disposition = "replace"
            else:
                write_disposition = "append"

            info = pipeline.run(source.with_resources(table_name), table_name=f"{table_name}_{db_name}", write_disposition=write_disposition)
            logging.info(f"Pipeline info: {info}")
            print(info)
            
            # Access the last_trace attribute from the pipeline
            last_trace = pipeline.last_trace

            # Get the row counts
            row_counts = last_trace.last_normalize_info.row_counts
            row_counts.pop('_dlt_pipeline_state', None)
            print(table_name, ":", "Row counts:", row_counts)
            logging.info(f"{table_name}: Row counts: {row_counts}")
            
            # Check if row_counts is empty
            if not row_counts:
                row_counts = 0
            else:
                # Extract the value from the dictionary if it's not empty
                table_name1 = f"{table_name}_{db_name}"
                row_counts = row_counts[table_name1] 

            engine1 = create_engine(db_credentials.connection_string)
            with engine1.connect() as conn:
                # Check if the table name exists
                query = text(f"SELECT COUNT(*) FROM dwh.multisource_log WHERE table_name = '{table_name}_{db_name}'")
                result = conn.execute(query)
                count = result.scalar()

                # If the table name exists, update the row count
                if count > 0:
                    query = text(f"UPDATE dwh.multisource_log SET row_processed = {row_counts}, modified_date = CURRENT_TIMESTAMP WHERE table_name = '{table_name}_{db_name}'")
                # If the table name does not exist, insert a new row
                else:
                    query = text(f"INSERT INTO dwh.multisource_log (table_name, row_processed) VALUES ('{table_name}_{db_name}', {row_counts})")
                conn.execute(query)
                conn.commit()
                
        # Record the end time
        end_time = pendulum.now()

        # Calculate the duration
        duration = end_time - start_time

        # Log the duration
        logging.info(f"Duration of the process: {duration.in_seconds()} seconds")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    engine = create_engine(db_credentials.connection_string)
    with engine.connect() as conn:
        query = text("select db_name, db_schema, credential, incremental_col, full_refresh from dwh.source_cred")
        result = conn.execute(query)
        rows = result.fetchall()
    for row in rows:
        db_name, db_schema, credential, incremental_col, full_refresh = row
        load_database_table(credential, db_name, incremental_col, full_refresh)
