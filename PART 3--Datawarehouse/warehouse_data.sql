-- run one by one 
-- import data in customer_staging after run schema.
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY AUTO_INCREMENT,
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    customer_segment VARCHAR(20)
);
-- insert data from csv into below schema
CREATE TABLE customers_staging (
    customer_id VARCHAR(10),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email varchar(100),
    registration_date varchar(100),
    city VARCHAR(50),
    phone varchar(30)
);
-- import in dim_customer
INSERT INTO dim_customer (
    customer_id,
    customer_name,
    city
)
SELECT DISTINCT
    customer_id,
    concat(first_name, '', last_name) as customer_name,
    city
FROM customers_staging
WHERE customer_id IS NOT NULL;

UPDATE dim_customer
SET customer_segment = 'Consumer'
WHERE customer_segment IS NULL OR customer_segment = '';

UPDATE dim_customer
SET 
    state = CASE city
        WHEN 'Bangalore' THEN 'Karnataka'
        WHEN 'Mumbai' THEN 'Maharashtra'
        WHEN 'Delhi' THEN 'Delhi'
        WHEN 'Hyderabad' THEN 'Telangana'
        WHEN 'Chennai' THEN 'Tamil Nadu'
        WHEN 'Pune' THEN 'Maharashtra'
        WHEN 'Kochi' THEN 'Kerala'
        WHEN 'Ahmedabad' THEN 'Gujarat'
        WHEN 'Jaipur' THEN 'Rajasthan'
        WHEN 'Kolkata' THEN 'West Bengal'
        WHEN 'Indore' THEN 'Madhya Pradesh'
        WHEN 'Chandigarh' THEN 'Chandigarh'
        WHEN 'Trivandrum' THEN 'Kerala'
        WHEN 'Lucknow' THEN 'Uttar Pradesh'
        ELSE 'Unknown'
    END;
    -- customer table end.
    
    
    -- run schema
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY AUTO_INCREMENT,
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    unit_price DECIMAL(10,2)
);

-- import data from csv file on this schema 
CREATE TABLE products_staging (
    product_id VARCHAR(10),
    product_name VARCHAR(100),
    category VARCHAR(50),
    price VARCHAR(20),
    stock_quantity VARCHAR(10)
);
-- after import now 
INSERT INTO dim_product (product_id, product_name, category, unit_price)
SELECT
    product_id,
    product_name,
    category,
    NULLIF(price, '')
FROM products_staging;

UPDATE dim_product
SET category = CONCAT(
    UPPER(LEFT(LOWER(category), 1)),
    SUBSTRING(LOWER(category), 2)
);
 
UPDATE dim_product
  SET subcategory = CASE
    -- Electronics subcategories
    WHEN category LIKE 'Electronics' AND product_name LIKE '%Samsung%' THEN 'Mobile'
    WHEN category LIKE 'Electronics' AND product_name LIKE '%iPhone%' THEN 'Mobile'
    WHEN category LIKE 'Electronics' AND product_name LIKE '%OnePlus%' THEN 'Mobile'
    WHEN category LIKE 'Electronics' AND product_name LIKE '%MacBook%' THEN 'Laptop'
    WHEN category LIKE 'Electronics' AND product_name LIKE '%HP Laptop%' THEN 'Laptop'
    WHEN category LIKE 'Electronics' AND product_name LIKE '%Dell Monitor%' THEN 'Monitor'
    WHEN category LIKE 'Electronics' AND product_name LIKE '%TV%' THEN 'TV'
    WHEN category LIKE 'Electronics' AND product_name LIKE '%Headphones%' THEN 'Audio'
    WHEN category LIKE 'Electronics' AND product_name LIKE '%Earbuds%' THEN 'Audio'
    
    -- Fashion subcategories
    WHEN category LIKE 'Fashion' AND product_name LIKE '%Shoes%' THEN 'Shoes'
    WHEN category LIKE 'Fashion' AND product_name LIKE '%Jeans%' THEN 'Jeans'
    WHEN category LIKE 'Fashion' AND product_name LIKE '%Shirt%' THEN 'Shirt'
    WHEN category LIKE 'Fashion' AND product_name LIKE '%T-Shirt%' THEN 'T-Shirt'
    WHEN category LIKE 'Fashion' AND product_name LIKE '%Trackpants%' THEN 'Trackpants'
    
    -- Groceries subcategories
    WHEN category LIKE 'Groceries' AND product_name LIKE '%Almonds%' THEN 'Nuts'
    WHEN category LIKE 'Groceries' AND product_name LIKE '%Rice%' THEN 'Rice'
    WHEN category LIKE 'Groceries' AND product_name LIKE '%Honey%' THEN 'Honey'
    WHEN category LIKE 'Groceries' AND product_name LIKE '%Dal%' THEN 'Pulses'

    ELSE 'Other'
END
WHERE subcategory IS NULL OR subcategory = ''
;
-- end 



-- transaction table 
CREATE TABLE transactions_staging (
    transaction_id VARCHAR(10),
    customer_id VARCHAR(10),
    product_id VARCHAR(10),
    quantity INT,
    unit_price DECIMAL(10,2),
    transaction_date VARCHAR(20),
    status VARCHAR(20)
);

-- import data from pc to above schema then

CREATE TABLE dim_transaction AS
SELECT
    transaction_id,
    NULLIF(customer_id, '') AS customer_id,
    NULLIF(product_id, '') AS product_id,
    quantity,
    unit_price,
    CASE
        WHEN transaction_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(transaction_date, '%Y-%m-%d')
        WHEN transaction_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
             AND CAST(SUBSTRING(transaction_date, 1, 2) AS UNSIGNED) > 12
            THEN STR_TO_DATE(transaction_date, '%d/%m/%Y')
        WHEN transaction_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
            THEN STR_TO_DATE(transaction_date, '%m/%d/%Y')
        WHEN transaction_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
             AND CAST(SUBSTRING(transaction_date, 1, 2) AS UNSIGNED) <= 12
            THEN STR_TO_DATE(transaction_date, '%m-%d-%Y')
        WHEN transaction_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
            THEN STR_TO_DATE(transaction_date, '%d-%m-%Y')
        ELSE NULL
    END AS transaction_date,
    status
FROM transactions_staging;

-- this for fill values of product_id.
UPDATE dim_transaction as t
JOIN products_staging as p
    ON t.unit_price = p.price
SET t.product_id = p.product_id
WHERE t.product_id IS NULL;
-- end 


-- dim_date
-- schema
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    day_of_month INT,
    month INT,
    month_name VARCHAR(10),
    quarter VARCHAR(2),
    year INT,
    is_weekend BOOLEAN
);

-- insert data
INSERT IGNORE INTO dim_date
SELECT DISTINCT
    DATE_FORMAT(transaction_date, '%Y%m%d') AS date_key,
    transaction_date AS full_date,
    DAYNAME(transaction_date),
    DAY(transaction_date),
    MONTH(transaction_date),
    MONTHNAME(transaction_date),
    CONCAT('Q', QUARTER(transaction_date)),
    YEAR(transaction_date),
    CASE 
        WHEN DAYOFWEEK(transaction_date) IN (1,7) THEN 1 
        ELSE 0 
    END
FROM dim_transaction
WHERE transaction_date IS NOT NULL;
-- end 


-- fact_sale
-- schema 
CREATE TABLE fact_sales (
    sale_key INT PRIMARY KEY AUTO_INCREMENT,
    date_key INT NOT NULL,
    product_key INT NOT NULL,
    customer_key INT NOT NULL,
    quantity_sold INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key)
    );
    
    -- data insert 
INSERT INTO fact_sales
(
    date_key,
    product_key,
    customer_key,
    quantity_sold,
    unit_price,
    discount_amount,
    total_amount
)
SELECT
    d.date_key,
    p.product_key,
    c.customer_key,
    t.quantity,
    t.unit_price,
    0 AS discount_amount,
    (t.quantity * t.unit_price) AS total_amount
FROM dim_transaction t
JOIN dim_date d
    ON d.full_date = t.transaction_date
JOIN dim_product p
    ON p.product_id = t.product_id
JOIN dim_customer c
    ON c.customer_id = t.customer_id
WHERE t.customer_id IS NOT NULL
  AND t.status = 'Completed';
  -- END

  drop tables customers_staging,products_staging,transactions_staging,dim_transaction;
  
  -- FINISH

