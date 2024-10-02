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
query ='select `Areas`,`PIN Code`,`latitude`,`longitude`,`storeid`,`servical` from zepto_lat_long_roshi'
df = pd.read_sql(query, conn)

# Close the database connection
conn.close()

output_file_path = fr'C:\\Users\\shalu.kumari\\PycharmProjects\\pythonProject\\zepto\\input\\zepto_lat_long_roshi.xlsx'

# Create the directory if it does not exist
df.to_excel(output_file_path, index=False)
print(f"Data has been exported to {output_file_path}")


