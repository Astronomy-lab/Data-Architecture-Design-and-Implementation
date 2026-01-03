-- Query 1: Monthly Sales Drill-Down
-- Business Scenario: "The CEO wants to see sales performance broken down by time periods. Start with yearly total, then quarterly, then monthly sales for 2024."
-- Demonstrates: Drill-down from Year to Quarter to Month
SELECT sum(fs.quantity_sold) as total_quantity, dd.year, dd.quarter, dd.month_name, SUM(fs.total_amount) AS total_sales from 
dim_date as dd
JOIN fact_sales AS fs
On dd.date_key = fs.date_key
GROUP BY  dd.year,
dd.quarter,
dd.month_name
order by dd.year,
dd.quarter,
dd.month_name ; 


-- Query 2: Top 10 Products by Revenue
-- Business Scenario:Business Scenario: "The product manager needs to identify top-performing products. Show the top 10 products by revenue,
-- along with their category, total units sold, and revenue contribution percentage."
-- Includes: Revenue percentage calculation
SELECT p.product_id, p.product_name,
SUM(f.quantity_sold) AS total_quantity,
SUM(f.total_amount) as total_revenue,
ROUND((SUM(f.total_amount) / SUM(SUM(f.total_amount)) over ()) * 100,2) AS revenue_percentage    -- 2 used for decimal point
from fact_sales f
join dim_product p
on f.product_key = p.product_key
group by p.product_id, p.product_name
order by total_revenue desc
LIMIT 10;


-- Query 3: Customer Segmentation
-- Business Scenario: Business Scenario: "Marketing wants to target high-value customers. Segment customers into 'High Value' (>₹50,000 spent),
-- 'Medium Value' (₹20,000-₹50,000), and 'Low Value' (<₹20,000). Show count of customers and total revenue in each segment."
-- Segments: High/Medium/Low value customers
select customer_segment,
    count(*) AS customer_count,
    sum(total_spent) AS total_revenue,
    round(avg(total_spent), 2) AS avg_revenue_per_customer
FROM ( SELECT c.customer_key,
	SUM(f.total_amount) as total_spent,
	CASE
		WHEN SUM(f.total_amount) > 50000 THEN 'High Value'
		WHEN SUM(f.total_amount) BETWEEN 20000 AND 50000 THEN 'Medium Value'
		ELSE 'Low Value'
	END as customer_segment
from fact_sales f
join dim_customer c
on f.customer_key = c.customer_key
group byc.customer_key
) customer_totals
group by customer_segment
order by total_revenue desc;

