# FlexiMart Data Architecture Project

**Student Name:**  --Aditya Vikram Singh--   

## Project Overview

In this Assignment, we will import data from Python and put up the query from MySQL to see if our data is correct or not.
And also we learn about MongoDB and how to put up a query without using MongoDB by using Python.
And we will be knowing what a star schema is, how it looks, and how to make one. Also, we see by the using of query. 

## Repository Structure
├── part1-database-etl/<br>
│   ├── etl_pipeline.py<br>
│   ├── schema_documentation.md<br>
│   ├── business_queries.sql<br>
│   └── data_quality_report.txt<br>
├── part2-nosql/<br>
│   ├── nosql_analysis.md<br>
│   ├── mongodb_operations.js<br>
│   └── products_catalog.json<br>
├── part3-datawarehouse/<br>
│   ├── star_schema_design.md<br>
│   ├── warehouse_schema.sql<br>
│   ├── warehouse_data.sql<br>
│   └── analytics_queries.sql<br>
└── README.md<br>


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

