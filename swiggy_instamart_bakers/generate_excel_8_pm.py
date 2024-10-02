from datetime import date

import pandas as pd
import os
today_date = date.today().strftime('%Y_%m_%d')

# # # # # # # # # ---------8 PM --------- # # # # # # # # #
input_file_path = fr'C:\\Palak\\LiveProjects\\swiggy_instamart_bakers\\data_files\\{today_date}\\swiggy_instamart_bakers_8_pm.json'

data = pd.read_json(input_file_path)

# Add an 'id' column starting from 1
# data.insert(0, "id", range(1, len(data) + 1))

# Replace empty strings with 'NA'
# data.replace('', 'NA', inplace=True)

# Define the desired column order

desired_order = [
    'platform', 'pincode', 'dateOfScrape', 'area', 'city',
    'productId', 'BrandName', 'CategoryName', 'productName',
    'productUrl', 'SkuName', 'productImage', 'mrp', 'productPrice',
    'discount', 'quantity', 'instock', 'others', 'variation_id', 'scraped_time'
]

# Reorder the DataFrame columns
df_reordered = data[desired_order]

# Define the output directory and file path
output_dir = fr'C:\\Palak\\LiveProjects\\swiggy_instamart_bakers\\data_files\\{today_date}'
output_file_path = os.path.join(output_dir, f'SwiggyInstamart_{today_date}_08_PM.xlsx')

# Ensure the directory exists
os.makedirs(output_dir, exist_ok=True)

# Save the DataFrame to an Excel file
df_reordered.to_excel(output_file_path, index=False, na_rep='NA')

print(f"Data has been successfully saved to {output_file_path}")


