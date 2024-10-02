from datetime import date, datetime
import pandas as pd
import os

# Get today's date in 'YYYY_MM_DD' format
# today_date = date.today().strftime('%Y_%m_%d')
def adjust_time(self):
    # Get the current time
    now = datetime.now()

    # Check the hour and adjust the time accordingly
    if now.hour < 10:
        # Set to the next hour, less than 10:00 AM
        next_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
    elif now.hour < 15:
        # Set to the next hour, less than 03:00 PM
        next_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
    elif now.hour < 20:
        # Set to the next hour, less than 08:00 PM
        next_time = now.replace(hour=20, minute=0, second=0, microsecond=0)

    # Format the time as requested
    formatted_time = next_time.strftime("%Y_%m_%d_%I_%p")

    return formatted_time


today_date_time = adjust_time()
# Define the input file path
input_file_path = fr'C:\\Palak\\LiveProjects\\swiggy_instamart_bakers\\data_files\\{today_date}\\swiggy_instamart_bakers_10_am.json'

# Load the JSON data into a DataFrame
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
output_file_path = os.path.join(output_dir, f'SwiggyInstamart_{today_date}_10_AM.xlsx')

# Ensure the directory exists
os.makedirs(output_dir, exist_ok=True)

# Save the DataFrame to an Excel file
df_reordered.to_excel(output_file_path, index=False, na_rep='NA')

print(f"Data has been successfully saved to {output_file_path}")


# # Save the reordered DataFrame to a new CSV file (or any other desired format)
# df_reordered.to_csv('reordered_data.csv', index=False)
#
# # You can also print the head of the DataFrame to verify the order
# print(df_reordered.head())

# # Load the JSON file
# # # # # # # # # ---------10 AM --------- # # # # # # # # #
# data = pd.read_json(fr'C:\Palak\LiveProjects\swiggy_instamart_bakers\data_files\{today_date}\swiggy_instamart_bakers_10_am.json')
# #
# # # Add an 'id' column
# data_id = [i + 1 for i in range(len(data))]
# data.insert(0, "id", pd.Series(data_id))
#
# # Replace empty strings with 'NA'
# data = data.replace('', 'NA', regex=True)
#
# # Define the output directory and file path
# output_dir = fr'C:\Palak\LiveProjects\swiggy_instamart_bakers\data_files\{today_date}'
# output_file = os.path.join(output_dir, 'swiggy_instamart_bakers_10_am.xlsx')
#
# # Ensure the directory exists
# os.makedirs(output_dir, exist_ok=True)
#
# # Save the DataFrame to an Excel file
# data.to_excel(output_file, index=False, na_rep='NA')
#
#
#
#
#
#
#
#
# #



