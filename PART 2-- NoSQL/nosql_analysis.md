NoSQL Justification Report

Section A: Limitations of RDBMS
RDBMS was designed to have a fixed schema where each table had defined columns and data types. This makes it challenging to work with products that have multiple attributes. For example, laptops would require fields for RAM and processor, but shoes would require fields for size and colour. Attempting to represent every possible attribute of a laptop and shoe within a single table would necessitate having a significant number of unused columns, increasing both storage space and complexity when running queries against that table.

Frequent changes to an RDBMS schema necessitate the addition of new types of products with their own sets of attributes, which creates challenges from a development perspective as modifying an existing table's structure (adding or removing columns) could potentially result in data loss due to disruption of existing data and the requirement for database downtime.

Concept or creating a hierarchy of data, such as customer reviews, which include rating, comment, and reply, also poorly supported within an RDBMS. In order to do include customer reviews in an RDBMS, it often requires multiple tables to store review and rating information, and the use of joins between those tables creates very large, complex queries that are inefficient.

Section B: NoSQL Benefits
MongoDB is an alternative type of database, we know as a traditional relational database (RDB). The primary difference is the type of schema used by another. In the case of MongoDB, it a dynamic schema. This allows for each document representing the same object/product, i.e. the computer and shoe, which have completely different fields. A laptop may contain fields such as RAM and CPU, whereas a shoe may contain size and colour—therefore the schema does not require any blanks.

The other advantage of MongoDB is allow for the use of embedded documents. You can create documents containing user reviews that are stored within the same document as the product itself. A single document would contain all user reviews and ratings, as well as comments and replies to each review. This allows for quick and easy reading and writing of reviews, as there is no need to use complex joins to associate reviews with products.

In addition to the above benefits, MongoDB also provides immense scalability horizontally. It allows the scaling of databases and their data over several physical servers. Consequently, the more the store grows, and as more products and reviews continue to accumulate, the less likely MongoDB will experience slowdowns due to lack of space or traffic congestion. Overall, MongoDB is a manageable and scalable solution to the limitations of traditional relational databases.

Section C: Trade-offs 
MongoDB's lack of strict relationships (as compared to MySQL) could lead to having inconsistent data unless care is taken when inserting data. An example of this may be a review that references a non-existent product.

Also, MongoDB may require more space because each document contains its own structure. Therefore, performing complex queries on multiple collections will take longer than in MySQL, as MySQL is designed to handle structured data and relationships.

Therefore, while MongoDB is very flexible in terms of how data can be inserted, it can be a bit less strict and may not be as efficient as MySQL for certain operations.