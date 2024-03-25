import dlt
from sqlalchemy import create_engine

tables = {
    "Production": ["Product", "Productcategory", "Productsubcategory"],
    "sales": ["currency", "currencyrate", "customer","salesorderdetail","salesorderheader","salesperson","salesterritory","store"],
}

    
engine = create_engine("mssql+pyodbc:///?odbc_connect=""DRIVER={ODBC Driver 17 for SQL Server};""SERVER=OUTCAST-BATES;""DATABASE=AdventureWorks2019;""trusted_connection=yes")

for schema,tables in tables.items():
    for table in tables:
            with engine.connect() as conn:
                query = f"SELECT * FROM {schema}.{table}"
                result = conn.execution_options(yield_per=100).exec_driver_sql(query)
                rows=result.fetchall()
                
                
            pipeline = dlt.pipeline(
            pipeline_name="from_database",
            destination="postgres",
            dataset_name="stg",
        )

            load_info = pipeline.run(map(lambda row: dict(row._mapping), rows), table_name=table,write_disposition="replace")

            print(load_info)