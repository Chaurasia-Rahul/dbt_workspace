import psycopg2
from psycopg2 import sql
import psycopg2.extras  # Import psycopg2 extras for DictCursor

# Function to connect to PostgreSQL database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="adventure_db",
            user="loader",
            password="Rahul_1234",
            host="localhost",
            port=5432
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to database:", e)
        return None

# Function to split table by year
def split_table_by_year():
    try:
        conn = connect_to_db()
        if conn is None:
            return

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)  # Use DictCursor

        # Query to select data from original table
        select_query = "SELECT * FROM stg.salesorderheader;"

        cur.execute(select_query)
        rows = cur.fetchall()

        # Iterate through rows, split by year, and insert into respective tables
        for row in rows:
            order_date = row['order_date']  # Accessing column by name
            year = order_date.year  # Extracting the year

            # Create table for the year if it doesn't exist
            create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} (LIKE stg.salesorderheader INCLUDING ALL);").format(sql.Identifier(f"sales_order_items_{year}"))
            cur.execute(create_table_query)

            # Insert row into the corresponding year's table
            insert_query = sql.SQL("INSERT INTO {} VALUES ({})").format(sql.Identifier(f"sales_order_items_{year}"), sql.SQL(', ').join(sql.Placeholder() * len(row)))
            cur.execute(insert_query, row)

        conn.commit()
        print("Table split operation completed successfully.")

    except psycopg2.Error as e:
        conn.rollback()
        print("Error:", e)

    finally:
        if conn is not None:
            cur.close()
            conn.close()

if __name__ == "__main__":
    split_table_by_year()
