import dlt
from sqlalchemy import create_engine, text
from sql_database import sql_database

def load_database_table(credential, db_name):
    source = sql_database(credentials=credential, schema='stg')
    for table_name in source.resources.keys():
        pipeline = dlt.pipeline(
            pipeline_name=db_name, destination="postgres", dataset_name="test_load_table"
        )
        info = pipeline.run(source.with_resources(table_name), table_name=f"{table_name}_{db_name}", write_disposition="replace")
        print(info)

        
if __name__ == "__main__":     
    engine = create_engine("postgresql://loader:Rahul_1234@localhost:5432/sakila_wh")
    with engine.connect() as conn:
        query = text("select db_name, db_schema,credential from dwh.source_cred")
        result = conn.execute(query)
        rows = result.fetchall()
        print(rows)
    for row in rows:
        db_name, db_schema, credential = row
        print(row)
        load_database_table(credential, db_name)