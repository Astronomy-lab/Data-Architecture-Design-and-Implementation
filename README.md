# FlexiMart Data Architecture Project

**Student Name:**  --Aditya Vikram Singh--  
**Student ID:**    --bitsom_ba_25071160--  
**Email:**         --vikadityasin@gmail.com--  
**Date:**          --06/01/2026--  

## Project Overview

In this Assignment, we will import data from Python and put up the query from MySQL to see if our data is correct or not.
And also we learn about MongoDB and how to put up a query without using MongoDB by using Python.
And we will be knowing what a star schema is, how it looks, and how to make one. Also, we see by the using of query. 

## Repository Structure
├── part1-database-etl/
│   ├── etl_pipeline.py
│   ├── schema_documentation.md
│   ├── business_queries.sql
│   └── data_quality_report.txt
├── part2-nosql/
│   ├── nosql_analysis.md
│   ├── mongodb_operations.js
│   └── products_catalog.json
├── part3-datawarehouse/
│   ├── star_schema_design.md
│   ├── warehouse_schema.sql
│   ├── warehouse_data.sql
│   └── analytics_queries.sql
└── README.md


## Technologies Used

- Python 3.12.10, pandas, mysql-connector-python
- MySQL Workbench 8.0 
- MongoDB  version 1.40.8

## Setup Instructions
### Database Setup

# Create databases
mysql -u root -p -e "CREATE DATABASE fleximart;"
mysql -u root -p -e "CREATE DATABASE fleximart_dw;"

# Run Part 1 - ETL Pipeline
python part1-database-etl/etl_pipeline.py

# Run Part 1 - Business Queries
mysql -u root -p fleximart < part1-database-etl/business_queries.sql

# Run Part 3 - Data Warehouse
mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_schema.sql
mysql -u root -p fleximart_dw < part3-datawarehouse/warehouse_data.sql
mysql -u root -p fleximart_dw < part3-datawarehouse/analytics_queries.sql


### MongoDB Setup
mongosh < part2-nosql/mongodb_operations.js

## Key Learnings
    By the help of this assignment, I have been able to learn many concepts and things include: 
    1. Usage of python(How to insert the data from python to mysql and how to query putup without accessing mysql.) 
    2. I have been able to learning NoSQL concepts and how to query and collect the data from mongo DB to python.
    In addition to that, I have been able to learn concepts of Data warehouse and how to build a star schema and how to insert the data inside the fact and dimension table.


## Challenges Faced

1.- Challenge: In this Assignment many things is new for me like MySQL connector code and Mongodb query by using of Python.
  - Solution: I take Mr. SUSHANT sir classes and youtube videoes also helped taken from ai due to many codes not wory perfectly.
2.- Challenge: Initially, I faced issues connecting Python to the MySQL database due to incorrect connection parameters.
  - Solution: I resolved this by double-checking the host, user, password, and database name in the connection string.  Additionally, I ensured that the MySQL server was running and accessible.
3.- Challenge: Hard to create table from Python 
  - Solution: I take tutorial classes and helping from youtube to learning.
4.- Challenge: MongoDB returns cursor objects, not tables.
  - Solution: So I used pd.DataFrame(list(result))
5.- Challenge: Many codes not worked.
  - Solution: Research and learn new code with the help of chatgpt and youtube also from Mr. SUSHANT SIR.
6.- Challenge: Many times MySQL code/script erase by mistake.
  - Solution: I prepare 2 query on it.If by mistake one erase then I will used another.
