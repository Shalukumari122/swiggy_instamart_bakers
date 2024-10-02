import pandas as pd
from sqlalchemy import create_engine

# Load data from both Excel files
df1 = pd.read_excel('product.xlsx')  # First file with pincode, area, city, store_id
df2 = pd.read_excel('swiggy_storeid_mapping.xlsx')  # Second file with Id, brand_name, sku_name, product_name, etc.

# Clean up column names by stripping spaces and converting to lowercase
df1.columns = df1.columns.str.strip()
df2.columns = df2.columns.str.strip()

# Preview the column names and data to ensure they match
print("Columns in df1:", df1.columns)
print("Columns in df2:", df2.columns)
print(df1.head())  # Check first few rows of df1
print(df2.head())  # Check first few rows of df2

# Initialize an empty DataFrame for the master table
master_table = pd.DataFrame()

# Iterate through each row of the first DataFrame
for index, row in df2.iterrows():
    # Extract relevant data from the first file (df1)
    pincode = row.get('pincode', 'NA')  # Use 'NA' if the column is missing
    area = row.get('area', 'NA')
    city = row.get('city', 'NA')
    store_id = row.get('store_id', 'NA')

    # Check if pincode, area, city, and store_id are being accessed correctly
    print(f"Processing row {index} from df1: {pincode}, {area}, {city}, {store_id}")

    # Iterate through the second DataFrame
    for i, product_row in df1.iterrows():
        # Combine the current row from df1 and df2
        combined_data = {
            'pincode': pincode,
            'area': area,
            'city': city,
            'store_id': store_id,
            'brand_name': product_row.get('brand_name', 'NA'),
            'sku_name': product_row.get('sku_name', 'NA'),
            'product_name': product_row.get('product_name', 'NA'),
            'product_id': product_row.get('product_id', 'NA'),
            'product_url': product_row.get('product_url', 'NA')
        }

        # Check the combined row before adding it
        print(f"Combined data row {i}: {combined_data}")

        # Convert the dictionary to a DataFrame and concatenate with the master table
        master_table = pd.concat([master_table, pd.DataFrame([combined_data])], ignore_index=True)

# SQL database connection setup
engine = create_engine('mysql+pymysql://root:actowiz@localhost/swiggy_instamart_bakers')

# Store the master table into SQL
master_table.to_sql('master_table', con=engine, if_exists='replace', index=False)

print("Data successfully inserted into SQL!")
