# =================================================================
# Import Libraries
# =================================================================

import pandas as pd
import numpy as np
import phonenumbers
import mysql.connector

# ---------------------------------------------------------
# Define 
# ---------------------------------------------------------

def read_raw_data(file_path):
    print(f"--- Loading file: {file_path}")
    return pd.read_csv(file_path)

def handle_missing_val(df):
    for column in df.columns:
        if df[column].isna().sum() > 0:
            # If it's a number, use the median (middle value)
            if df[column].dtype in ["int64", "float64"]:
                df[column] = df[column].fillna(df[column].median())
            # If it's text, just use the most common entry
            else:
                df[column] = df[column].fillna(df[column].mode()[0])
    return df

# connect to sql and upload data
def upload_to_mysql(df, table_name, db_name="demo", user="root", password="Adi0506tyaa@"):
    print(f"--- Starting upload for table: {table_name}")
    try:
        # connect to the sql
        db_connection = mysql.connector.connect(
            host="localhost",
            user=user,
            password=password
        )
        my_cursor = db_connection.cursor()

        # set up the databse
        my_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        my_cursor.execute(f"USE {db_name}")

        # sql types for each column
        sql_columns = []
        for col in df.columns:
            if "date" in col.lower():
                sql_columns.append(f"{col} DATE")
            elif df[col].dtype in ["int64", "float64"]:
                sql_columns.append(f"{col} INT")
            else:
                sql_columns.append(f"{col} VARCHAR(255)")

        # create or clear the database table
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(sql_columns)})"
        my_cursor.execute(create_query)
        my_cursor.execute(f"TRUNCATE TABLE {table_name}")

        # insert data into the table
        column_names = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

        # Replace NaN with None for SQL compatibility
        cleaned_for_sql = df.where(pd.notnull(df), None)
        
        my_cursor.executemany(insert_query, cleaned_for_sql.values.tolist())
        db_connection.commit()
        
        print(f"--- Done! {table_name} is updated.")

    except mysql.connector.Error as error_msg:
        print(f"!!! Error occurred: {error_msg}")

    finally:
        if 'my_cursor' in locals(): my_cursor.close()
        if 'db_connection' in locals(): db_connection.close()


# ---------------------------------------------------------
# NOW CLEAN THE CUSTOMERS DATA
# ---------------------------------------------------------
print("\n[PROCESS 1: CUSTOMERS]")

customer_path = r"C:\Users\Lenovo\OneDrive\Documents\GitHub\Data\customers_raw.csv"
customers = read_raw_data(customer_path)
customers = handle_missing_val(customers)
customers = customers.drop_duplicates()

# Remove duplicate emails
if "email" in customers.columns:
    customers = customers.drop_duplicates(subset=["email"])

#   Fix date formats to SQL standard (YYYY-MM-DD)
customers["registration_date"] = (
    pd.to_datetime(customers["registration_date"], errors="coerce")
    .dt.strftime("%Y-%m-%d")
    .fillna("1900-01-01")
)

# Standardize phone numbers to E.164 format
valid_phones = []
for p in customers["phone"]:
    try:
        parsed_p = phonenumbers.parse(str(p), "IN")
        valid_phones.append(phonenumbers.format_number(parsed_p, phonenumbers.PhoneNumberFormat.E164))
    except:
        valid_phones.append(None)
customers["phone"] = valid_phones

# Convert C001-1
customers["customer_id"] = customers["customer_id"].astype(str).str.extract(r"(\d+)").astype(int)


# ---------------------------------------------------------
# Now CLEAN THE PRODUCTS DATA
# ---------------------------------------------------------
print("\n[PROCESS 2: PRODUCTS]")

product_path = r"C:\Users\Lenovo\OneDrive\Documents\GitHub\Data\Product_raw.csv"
products = read_raw_data(product_path)
products = handle_missing_val(products)
products = products.drop_duplicates()

# Standardize category names to title case
products["category"] = products["category"].astype(str).str.title()

# Clean product IDs
products["product_id"] = products["product_id"].astype(str).str.extract(r"(\d+)").astype(int)


# ---------------------------------------------------------
# Now CLEAN THE SALES DATA & MAKE EXTRA TABLES
# ---------------------------------------------------------
print("\n[PROCESS 3: SALES & ORDERS]")

sales_path = r"C:\Users\Lenovo\OneDrive\Documents\GitHub\Data\sales_raw.csv"
sales = pd.read_csv(sales_path)

sales = sales.drop_duplicates()
sales.fillna(0, inplace=True)

# Fix transaction dates to SQL format
sales["order_date"] = (
    pd.to_datetime(sales["transaction_date"], format="%d/%m/%Y", errors="coerce")
    .dt.strftime("%Y-%m-%d")
    .fillna("1900-01-01")
)

# Clean IDs and set proper number types
sales["customer_id"] = sales["customer_id"].astype(str).str.extract(r"(\d+)").astype(int)
sales["product_id"] = sales["product_id"].astype(str).str.extract(r"(\d+)").astype(int)
sales["quantity"] = sales["quantity"].astype(int)
sales["unit_price"] = sales["unit_price"].astype(float)

# Calculate total for each line
sales["subtotal"] = sales["quantity"] * sales["unit_price"]

# --- Create the 'orders' table (one row per customer/date) ---
orders = sales.groupby(["customer_id", "order_date"], as_index=False).agg({"subtotal": "sum"})
orders.rename(columns={"subtotal": "total_amount"}, inplace=True)
orders["status"] = "Pending"
orders = orders.drop_duplicates(subset=["customer_id", "order_date"])

# --- Create the 'order_items' table (summary of products sold) ---
order_items = (sales
    .groupby(["product_id", "unit_price"], as_index=False)
    .agg({"quantity": "sum", "subtotal": "sum"})
    .sort_values("product_id")
    .reset_index(drop=True)
)


# ---------------------------------------------------------
#  FINAL UPLOAD TO DATABASE
# ---------------------------------------------------------
print("\n[FINAL STEP: DATABASE UPLOAD]")

upload_to_mysql(customers, "customers")
upload_to_mysql(products, "products")
upload_to_mysql(orders, "orders")
upload_to_mysql(order_items, "order_items")

print("\nETL PIPELINE COMPLETED SUCCESSFULLY! ALL DATA IS IN MYSQL.")




