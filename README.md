# SQL2NOSQL<br>
# Relational Database to NoSQL Conversion by Schema Migration and Mapping(https://ijcert.org/ems/ijcert_papers/V3I909.pdf) <br>
To build a Schema-Migration and Mapping Framework using python to support automatic data migration from relational databases to NoSQL.<br>
The Schema Migration class automates the migration of data from an SQLite database to Mongo DB, preserving relationships and dependencies between tables through foreign key analysis.<br>
The Data mapping system is designed to facilitate the execution of SQL queries on a NoSQL database.<br>

# Project Group: 46
Aparna Phundir         - M23AID006<br>
Abhishek Kumar Gupta   - M22AIE237<br>
Bhuvaneswari J         - M23AID053<br>

# Prerequisites
Sqlite <br>
MongoDB <br>
Python 3.x <br>
xml.etree.ElementTree <br>
Sqlparse <br>
Pymongo <br>
re <br>

# Schema Migration
The SchemaMigration class automates the migration of data from an SQLite database to MongoDB, preserving relationships and dependencies between tables through foreign key analysis. The migration involves:<br>

Extracting the schema and table relationships.<br>
Determining the order of table migrations based on dependencies.<br>
Renaming columns for clarity during migration.<br>
Transferring data from SQLite to MongoDB.<br>

![image](https://github.com/user-attachments/assets/179dc575-0b2f-4e28-8e3a-1bb9b5155005)

# Data Mapping
The SQL-to-NoSQL Query Translation and Execution System is designed to facilitate the execution of SQL queries on a NoSQL database. This is achieved by intercepting SQL queries, converting them to a NoSQL equivalent format, and executing them on a MongoDB database. The system comprises three core components: the Mediator, the Convert module, and the Database Metadata.<br>
Enable seamless integration of SQL-based applications with NoSQL databases.<br>
Translate SQL queries into NoSQL queries while preserving the semantic integrity of the operations.<br>
Support CRUD operations (SELECT, INSERT, UPDATE, DELETE).<br>
Provide formatted results for easier integration with relational systems.<br>

![image](https://github.com/user-attachments/assets/4709dc32-9e3e-43c6-b647-f68d1d13ce95)

# Execution
Create DB in Sqlite with table structure and data.<br>
Run mongo db using mongod command in command prompt.<br>
Execute schema migration framework python code to migrate sqlite db data to mongo db.<br>
Execute mapping framework python code to to facilitate the execution of SQL queries on a NoSQL database. <br>
Execute sql2nosql_plot.py to validate performance of sqlite and mongodb transaction.<br>

# Output-Performance comparison diagram:
![image](https://github.com/user-attachments/assets/7b0ae63d-3aca-428b-b71e-2d980f768fa9)









