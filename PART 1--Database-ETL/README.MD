# FlexiMart ETL Project

## Overview
This project demonstrates a **complete ETL (Extract, Transform, Load) pipeline** for a retail company called **FlexiMart**. The project uses Python to clean and load customer, product, and sales data from CSV files into a MySQL database.  

After loading the data, we also write SQL queries to answer specific business questions about customer purchases, product sales, and monthly trends.

## Project Structure

FlexiMart_ETL/
│
├── etl_pipeline.py # Main Python ETL script
├── customers_raw.csv # Raw customer data
├── products_raw.csv # Raw product data
├── sales_raw.csv # Raw sales data
├── business_queries.sql # SQL queries for business questions
├── schema_documentation.md# Documentation of database schema
├── data_quality_report.txt# Report on data cleaning steps
└── README.md # Project overview and instructions

markdown
Copy code

## ETL Pipeline (Python)

The ETL pipeline is implemented in `etl_pipeline.py` and follows these steps:

1. **Extract**
   - Read `customers_raw.csv`, `products_raw.csv`, and `sales_raw.csv` using **pandas**.

2. **Transform**
   - Remove duplicate rows.
   - Handle missing values:
     - Fill missing numeric values with median.
     - Fill missing categorical values with mode.
   - Standardize phone numbers (e.g., `+91-9876543210`).
   - Standardize product categories (e.g., "electronics" → "Electronics").
   - Convert dates to `YYYY-MM-DD`.
   - Calculate subtotal for sales (`quantity × unit_price`).
   - Generate surrogate keys (auto-increment IDs in MySQL).

3. **Load**
   - Insert cleaned data into MySQL tables:
     - `customers`
     - `products`
     - `orders`
     - `order_items`
   - Handles order ID mapping for `order_items`.

## Database Schema

The database `fleximart` has **4 tables**:

1. **customers**
   - Stores customer info.
   - Attributes: `customer_id`, `first_name`, `last_name`, `email`, `phone`, `city`, `registration_date`.
   - Relationship: One customer can place many orders.

2. **products**
   - Stores product info.
   - Attributes: `product_id`, `product_name`, `category`, `price`, `stock_quantity`.

3. **orders**
   - Stores order summary.
   - Attributes: `order_id`, `customer_id`, `order_date`, `total_amount`, `status`.
   - Relationship: Linked to customers (many orders per customer).

4. **order_items**
   - Stores detailed items per order.
   - Attributes: `order_item_id`, `order_id`, `product_id`, `quantity`, `unit_price`, `subtotal`.
   - Relationships: Linked to orders and products.

## Data Quality Report

The ETL script also generates `data_quality_report.txt` showing:

- Number of records processed per file.
- Number of duplicates removed.
- Number of missing values handled.
- Number of records successfully loaded into the database.

## Business Queries

All SQL queries are in `business_queries.sql`:

1. **Customer Purchase History**
   - Lists customers with 2+ orders and total spent > ₹5,000.
   - Columns: `customer_name`, `email`, `total_orders`, `total_spent`.

2. **Product Sales Analysis**
   - Shows category-wise product count, total quantity sold, and total revenue.
   - Only categories with revenue > ₹10,000 are included.

3. **Monthly Sales Trend**
   - Displays monthly total orders, revenue, and cumulative revenue for 2024.

## How to Run

1. Install Python dependencies:

```bash
pip install -r requirements.txt
Update MySQL credentials in etl_pipeline.py:

python
Copy code
user="root"
password="YOUR_PASSWORD"
host="localhost"
database="demo"  # will create fleximart database
Run the ETL script:

bash
Copy code
python etl_pipeline.py
Check data_quality_report.txt for cleaning details.

Run SQL queries in MySQL to analyze business questions:

sql
Copy code
source business_queries.sql;
