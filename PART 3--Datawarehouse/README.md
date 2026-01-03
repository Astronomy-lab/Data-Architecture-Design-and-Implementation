Schema Design
The data warehouse uses a Star Schema consisting of:

🔹 Fact Table

fact_sales
Stores measurable business events such as:
Quantity sold
Unit price
Total sales amount
Each row represents a transaction line item.

🔹 Dimension Tables

dim_date – Time attributes (day, month, quarter, year, weekend)
dim_product – Product details (name, category, price)
dim_customer – Customer details (name, city, segment)

This design enables fast analytical queries and easy reporting.

🎯 Design Decisions (Summary)

Granularity:
Transaction-level data was chosen to allow detailed analysis and flexible aggregation.

Surrogate Keys:
Integer surrogate keys improve performance, ensure stability, and simplify joins.

Drill-Down & Roll-Up Support:
The date dimension allows analysis at year → quarter → month → day levels.

🔄 Sample Data Flow (Example)
Source Transaction
Order rgba(0, 11, 17, 1)
Customer: John Doe
Product: Laptop
Quantity: 2
Price: ₹50,000

In the Data Warehouse
fact_sales --

date_key: 20240115
product_key: 5
customer_key: 12
quantity_sold: 2
unit_price: 50000
total_amount: 100000


dim_product --

product_key: 5
product_name: Laptop
category: Electronics


dim_customer --

customer_key: 12
customer_name: John Doe
city: Mumbai

📈 Analytical Use Cases (Examples)
1️⃣ Time-Based Sales Analysis
Analyze sales performance using Year → Quarter → Month drill-down.

SELECT year, quarter, month_name, SUM(total_amount) AS total_sales
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY year, quarter, month_name;


2️⃣ Top Product Performance
Identify top 10 products by revenue and their contribution to total sales.

SELECT product_name, SUM(total_amount) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 10;


3️⃣ Customer Value Segmentation
Classify customers into High, Medium, and Low Value segments.

CASE
  WHEN total_spent > 50000 THEN 'High Value'
  WHEN total_spent BETWEEN 20000 AND 50000 THEN 'Medium Value'
  ELSE 'Low Value'
END


✅ Key Learning Outcomes
Dimensional modeling using Star Schema
Use of surrogate keys
OLAP concepts: drill-down & roll-up
Business-oriented SQL analytics