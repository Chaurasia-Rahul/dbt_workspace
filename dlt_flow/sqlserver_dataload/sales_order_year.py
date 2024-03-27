import dlt
from sqlalchemy import create_engine, text
from dlt import resource

# Connect to your PostgreSQL database
engine = create_engine('postgresql://loader:Rahul_1234@localhost:5432/adventure_db')

# Get unique years
with engine.connect() as connection:
    result = connection.execute(text("SELECT DISTINCT EXTRACT(YEAR FROM order_date) AS year FROM stg.salesorderheader"))
    years = [row.year for row in result]
    print(result)   
    
