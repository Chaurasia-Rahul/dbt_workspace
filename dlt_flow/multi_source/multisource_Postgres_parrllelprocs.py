#multisource_Postgres_parrllelprocs
import dlt
from sqlalchemy import create_engine, text
from sql_database import sql_database
import multiprocessing
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)   ## start time and end time
logger = logging.getLogger(__name__)

try:
    def load_database_table(credential, db_name, db_schema):
        logger.info(f"Loading table {db_schema}.{db_name} started.")
        start_time = time.time()
        pipeline = dlt.pipeline(
            pipeline_name=db_name, destination="postgres", dataset_name="test_load_parllel"
        )
        source = sql_database(credentials=credential, schema=db_schema)
        info = pipeline.run(source, write_disposition="replace")
        end_time = time.time()
        logger.info(f"Loading table {db_schema}.{db_name} completed in {end_time - start_time:.2f} seconds.")

    def process_table_row(row):
        db_name, db_schema, credential = row
        load_database_table(credential, db_name, db_schema)

    if __name__ == "__main__":
        engine = create_engine("postgresql://loader:Rahul_1234@localhost:5432/sakila_wh")
        with engine.connect() as conn:
            query = text("select db_name, db_schema, credential from dwh.source_cred")
            result = conn.execute(query)
            rows = result.fetchall()
            logger.info("Fetched rows from database:")
            logger.info(rows)

        # Create a multiprocessing pool
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        # Map the processing function to each row
        pool.map(process_table_row, rows)
        # Close the pool to release resources
        pool.close()
        pool.join()

except Exception as e:
    logger.error("An error occurred:", exc_info=True)
