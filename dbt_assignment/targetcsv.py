import psycopg2
import csv
import os
import random
from decimal import Decimal

# Establish a connection to the database
conn = psycopg2.connect(
    dbname="adventure_db",
    user="loader",
    password="Rahul_1234",
    host="localhost",
    port=5432
)

# Create a cursor object
cur = conn.cursor()

# Execute your SQL query
cur.execute("""SELECT 
        sales_person_id,
        EXTRACT(YEAR FROM Order_Date) AS Year,
        round(AVG(sub_total),2) AS AverageSales
    FROM 
        stg.salesorderheader
    GROUP BY 
        sales_person_id, EXTRACT(YEAR FROM Order_Date)
    ORDER BY 
        EXTRACT(YEAR FROM Order_Date), sales_person_id """)

# Fetch all rows from the result set
rows = cur.fetchall()

folder_path = "F:\\B2B\\INFLOW\\dbt_workspace\\dbt_assignment\\seeds"
# Define the path for the CSV file
csv_file_path = os.path.join(folder_path, "targetsales.csv")

# Write the data to a CSV file
with open(csv_file_path, "w", newline="") as csv_file:
    csv_writer = csv.writer(csv_file)
    # Write the header row if needed
    csv_writer.writerow(["SalesPersonID", "Year", "AverageSales", "Target"])
    # Write the data rows
    for row in rows:
        sales_person_id, year, average_sales = row
        random_increase = Decimal(random.uniform(0.10, 0.15))
        target = average_sales * (1 + random_increase)  # Target with random increase
        # Round the target value to two decimal places
        target = round(target, 2)
        csv_writer.writerow([sales_person_id, year, average_sales, target])

# Close the cursor and the connection
cur.close()
conn.close()
