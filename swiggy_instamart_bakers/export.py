import os
from datetime import datetime, timedelta

import pandas as pd
import pymysql

# Connect to the database
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='actowiz',
    database='swiggy_instamart_bakers'
)


def adjust_time():
    # Get the current time
    now = datetime.now()

    # Check the hour and adjust the time accordingly
    if now.hour < 10:
        # Set to 10:00 AM today
        next_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
    elif now.hour < 15:
        # Set to 03:00 PM today
        next_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
    elif now.hour < 20:
        # Set to 08:00 PM today
        next_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
    else:
        # Set to 12:00 AM (midnight) the next day
        next_time = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Format the time as requested
    formatted_time = next_time.strftime("%Y_%m_%d_%I_%p")

    return formatted_time

today_date_time = adjust_time()

# Query to select the data
query = f"""
SELECT `Sr.No`, platform, pincode, dateOfScrape,area, city,storeid,  productId, BrandName, 
CategoryName, productName, productUrl, SkuName, productImage, mrp, productPrice, 
discount, quantity, instock, others, variation_id, scraped_time 
FROM product_data{today_date_time}
"""  # Ensure the table name is correct with the date and time suffix
# FROM product_data{today_date_time}
# Use pandas to execute the query and store the result in a DataFrame
df = pd.read_sql(query, conn)

# Close the database connection
conn.close()

# Specify the directory to save the Excel file
output_dir = fr'C:\\Shalu\\LiveProjects\\swiggy_instamart_bakers\\data_files\\{today_date_time}'

# Create the directory if it does not exist
os.makedirs(output_dir, exist_ok=True)

# Specify the file path to save the Excel file
output_file_path = os.path.join(output_dir, f'SwiggyInstamart_{today_date_time}.xlsx')

# Export the DataFrame to an Excel file
df.to_excel(output_file_path, index=False)

print(f"Data has been exported to {output_file_path}")
