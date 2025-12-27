# =================================================
# IMPORT LIBRARIES
# =================================================
import pandas as pd
import numpy as np
import phonenumbers
import mysql.connector

# =================================================
# READ CSV FILE
# =================================================
def read_raw_data(file_path):
    return pd.read_csv(file_path)


# =================================================
# HANDLE MISSING VALUES
# =================================================
def handle_missing_val(df):
    for col in df.columns:
        if df[col].isna().sum() > 0:
            if df[col].dtype in ["int64", "float64"]:
                df[col] = df[col].fillna(df[col].median())
            else:
                df[col] = df[col].fillna(df[col].mode()[0])
    return df


# =================================================
# UPLOAD DATA TO MYSQL
# =================================================
def upload_to_mysql(df, table_name, db_name="demo", user="root", password="Adi0506tyaa@"):
    conn = None
    cursor = None
    try:
        # -----------------------------
        # Connect to MySQL
        # -----------------------------
        conn = mysql.connector.connect(
            host="localhost",
            user=user,
            password=password
        )
        cursor = conn.cursor()

        # -----------------------------
        # Create database
        # -----------------------------
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cursor.execute(f"USE {db_name}")

        # -----------------------------
        # Create table dynamically
        # -----------------------------
        columns_sql = []
        for col in df.columns:
            if "date" in col.lower():
                columns_sql.append(f"{col} DATE")
            else:
                if df[col].dtype in ["int64", "float64"]:
                 columns_sql.append(f"{col} INT")
                else:
                 columns_sql.append(f"{col} VARCHAR(255)")


        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(columns_sql)}
        )
        """
        cursor.execute(create_table_sql)

        # -----------------------------
        # Prepare INSERT query
        # -----------------------------
        columns = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Convert NaN → None for MySQL
        df = df.where(pd.notnull(df), None)

        cursor.executemany(insert_sql, df.values.tolist())
        conn.commit()

        print(f"Data uploaded successfully into table '{table_name}'")

    except mysql.connector.Error as err:
        print("MySQL Error:", err)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =================================================
# CUSTOMER DATA CLEANING
# =================================================
print("\n---- CUSTOMERS DATA ----")
cust_file = r"C:\Users\Lenovo\OneDrive\Documents\GitHub\customers_raw.csv"

clean_cust_df = read_raw_data(cust_file)
clean_cust_df = handle_missing_val(clean_cust_df)
clean_cust_df = clean_cust_df.drop_duplicates()

# Remove duplicates based on email 
if "email" in clean_cust_df.columns:
    clean_cust_df = clean_cust_df.drop_duplicates(subset=["email"])



# Clean registration_date
if "registration_date" in clean_cust_df.columns:
    clean_cust_df["registration_date"] = (
        pd.to_datetime(clean_cust_df["registration_date"], errors="coerce")
        .dt.strftime("%Y-%m-%d")
        .fillna("1900-01-01")
    )

# Clean phone numbers
if "phone" in clean_cust_df.columns:
    phone_list = []
    for num in clean_cust_df["phone"]:
        try:
            phone = phonenumbers.parse(str(num), "IN")
            phone_list.append(
                phonenumbers.format_number(phone, phonenumbers.PhoneNumberFormat.E164)
            )
        except:
            phone_list.append(None)
    clean_cust_df["phone"] = phone_list

print("Customers cleaned:", clean_cust_df.shape)

# Convert customer_id from alphanumeric to INT
if "customer_id" in clean_cust_df.columns:
    clean_cust_df["customer_id"] = (
        clean_cust_df["customer_id"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(int)
    )



# =================================================
# PRODUCT DATA CLEANING
# =================================================
print("\n---- PRODUCTS DATA ----")
prod_file = r"C:\Users\Lenovo\OneDrive\Documents\GitHub\Product_raw.csv"

clean_prod_df = read_raw_data(prod_file)
clean_prod_df = handle_missing_val(clean_prod_df)
clean_prod_df = clean_prod_df.drop_duplicates()

if "category" in clean_prod_df.columns:
    clean_prod_df["category"] = clean_prod_df["category"].astype(str).str.title()

print("Products cleaned:", clean_prod_df.shape)

# Convert product_id from alphanumeric to INT
if "product_id" in clean_prod_df.columns:
    clean_prod_df["product_id"] = (
        clean_prod_df["product_id"]
        .astype(str)
        .str.extract(r"(\d+)")
        .astype(int)
    )



print("---- SALES DATA ----")

# 1. Read CSV file
sales_df = pd.read_csv(
    r"C:\Users\Lenovo\OneDrive\Documents\GitHub\sales_raw.csv"
)

# 2. Remove duplicate rows
sales_df = sales_df.drop_duplicates()

# 3. Handle missing values
sales_df.fillna(0, inplace=True)

# 4. Convert transaction_date to order_date (FIXED)
sales_df["order_date"] = pd.to_datetime(
    sales_df["transaction_date"],
    format="%d/%m/%Y",   # <-- THIS IS THE FIX
    errors="coerce"
).dt.strftime("%Y-%m-%d")

sales_df["order_date"].fillna("1900-01-01", inplace=True)


# 5. Convert customer_id & product_id to numbers
sales_df["customer_id"] = (
    sales_df["customer_id"]
    .astype(str)
    .str.extract(r"(\d+)")
    .astype(int)
)

sales_df["product_id"] = (
    sales_df["product_id"]
    .astype(str)
    .str.extract(r"(\d+)")
    .astype(int)
)

# 6. Convert quantity & unit_price
sales_df["quantity"] = sales_df["quantity"].astype(int)
sales_df["unit_price"] = sales_df["unit_price"].astype(float)

# 7. Calculate subtotal (for order_items table)
sales_df["subtotal"] = (
    sales_df["quantity"] * sales_df["unit_price"]
)

print("Cleaned sales rows:", sales_df.shape[0])

# ----------------------------
# Orders table data
# ----------------------------
orders_df = (
    sales_df
    .groupby(["customer_id", "order_date"], as_index=False)
    ["subtotal"].sum()
)

orders_df.rename(
    columns={"subtotal": "total_amount"},
    inplace=True
)

orders_df["status"] = "Pending"

print("\nOrders Data:")
print(orders_df.head())

# ----------------------------
# Order Items table data
# ----------------------------
order_items_df = sales_df[
    ["product_id", "quantity", "unit_price", "subtotal"]
]

print("\nOrder Items Data:")
print(order_items_df.head())




# =================================================
# UPLOAD TO MYSQL
# =================================================
upload_to_mysql(clean_cust_df, "customers")
upload_to_mysql(clean_prod_df, "products")
upload_to_mysql(orders_df, "orders")
upload_to_mysql(order_items_df, "order_items")


