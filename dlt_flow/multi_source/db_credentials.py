from sqlalchemy import create_engine

# Define the credentials
db_credentials = {
    'username': 'loader',
    'password': 'Rahul_1234',
    'host': 'localhost',
    'port': '5432',
    'database': 'dlt_data'
}

# Construct the connection string
connection_string = f"postgresql://{db_credentials['username']}:{db_credentials['password']}@{db_credentials['host']}:{db_credentials['port']}/{db_credentials['database']}"

# Create the engine
#engine = create_engine(connection_string)

# Define the file path
log_file_path = 'F:\\B2B\\INFLOW\\dbt_workspace\\dlt_flow\\multi_source\\pipeline_trace.log'
