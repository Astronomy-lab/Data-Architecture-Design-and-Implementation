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
images---
MongoDb queries with using of Python
<img width="909" height="781" alt="Screenshot 2026-06-02 203443" src="https://github.com/user-attachments/assets/56ba1d6f-2b26-466a-9005-c3ca874e330d" />

<img width="879" height="764" alt="Screenshot 2026-06-02 203450" src="https://github.com/user-attachments/assets/5f680691-2b86-4d7c-8244-51fc423bce34" />

<img width="860" height="751" alt="Screenshot 2026-06-02 203502" src="https://github.com/user-attachments/assets/b284db3b-347d-43e1-bdc9-47be19a9b107" />

<img width="625" height="749" alt="Screenshot 2026-06-02 203511" src="https://github.com/user-attachments/assets/bd6d68b0-a048-4b2a-8b49-f471444cc06f" />

<img width="651" height="373" alt="Screenshot 2026-06-02 203519" src="https://github.com/user-attachments/assets/f94f555a-e524-442f-8248-2eb39378eebd" />


MongoDb data-
<img width="679" height="797" alt="Screenshot 2026-06-02 203426" src="https://github.com/user-attachments/assets/773a1842-8253-424a-91e3-b2fdcb43a2ea" />

SQL Query
<img width="457" height="621" alt="Screenshot 2026-06-02 203711" src="https://github.com/user-attachments/assets/f5951018-bc12-4885-80c9-67ce25ee47af" />



## Key Learnings
    By the help of this assignment, I have been able to learn many concepts and things include: 
    1. Usage of python(How to insert the data from python to mysql and how to query putup without accessing mysql.) 
    2. I have been able to learning NoSQL concepts and how to query and collect the data from mongo DB to python.
    In addition to that, I have been able to learn concepts of Data warehouse and how to build a star schema and how to insert the data inside the fact and dimension table.

