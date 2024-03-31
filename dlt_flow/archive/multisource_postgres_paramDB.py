import dlt
from sqlalchemy import create_engine,text
from sql_database import sql_database

def load_select_tables_from_database(pipeline_name,schema, resources) -> None:
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name, destination="postgres", dataset_name="multisource_data"
    )
    source = sql_database(schema=schema).with_resources(*resources)
    info = pipeline.run(source, write_disposition="replace")
    print(info)

if __name__ == "__main__":
    # Connect to the database and fetch parameters
    engine = create_engine("postgresql://loader:Rahul_1234@localhost:5432/sakila_wh")
    with engine.connect() as conn:
        query = text("""
            SELECT 
                pipeline_name,
                db_schema,
                STRING_AGG(resources, ', ') AS resources 
            FROM 
                dwh.param_table 
            GROUP BY 
                pipeline_name, db_schema
        """)
        result = conn.execute(query)
        rows = result.fetchall()
        print(rows)
    for row in rows:
        pipeline_name, schema, resources = row
        resources = resources.split(', ')
        load_select_tables_from_database(pipeline_name, schema, resources)