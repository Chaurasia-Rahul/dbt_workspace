import pandas as pd
import random

# Sample data for demonstration
sales_data = {
    'year': [2011, 2012, 2013, 2014],
    'salespersonid': [274, 275, 276, 278,279,280,281,282],
    'revenue': [0, 1750406, 1439156, 1997186, 1620276,1849640,1927059,2073505,2038234]
}

df_sales = pd.DataFrame(sales_data)

# Calculate average revenue for each salespersonid
avg_revenue = df_sales.groupby('salespersonid')['revenue'].mean()

# Randomize target within 10% to 15% of the average revenue
def calculate_target(avg_rev):
    return avg_rev * (1 + random.uniform(0.10, 0.15))

# Apply randomization to calculate target
df_sales['target'] = df_sales['salespersonid'].map(avg_revenue).map(calculate_target)

# Save DataFrame to CSV
df_sales.to_csv('targets.csv', index=False)

print("CSV file 'targets.csv' has been created with the calculated targets:")
print(df_sales)
