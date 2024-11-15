import sqlite3
import pymongo
import time
import matplotlib.pyplot as plt
import numpy as np

# Database connections
sqlite_conn = sqlite3.connect('example.db')  # SQLite database
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")  # MongoDB connection
mongo_db = mongo_client["migrated_db"]
mongo_collection = mongo_db["users"]

# Define transaction sizes to test
transaction_sizes = [500, 1000, 5000, 10000]

# Store times for SQLite and MongoDB
sql_times = []
nosql_times = []

# Define a function to run a query on SQLite and measure time
def sqlite_query(transaction_size):
    start_time = time.time()
    cursor = sqlite_conn.cursor()
    # Example query: Adjust the table and column names as needed
    #cursor.execute(f"SELECT * FROM users u JOIN orders o ON u.id = o.user_id LIMIT {transaction_size}")
    cursor.execute(f"SELECT * FROM users LIMIT {transaction_size}")
    results = cursor.fetchall()
    end_time = time.time()
    return end_time - start_time

# Define a function to run a query on MongoDB and measure time
def mongodb_query(transaction_size):
    start_time = time.time()
    # Example query: Adjust the collection and filter as needed
    results = list(mongo_collection.find().limit(transaction_size))
    end_time = time.time()
    return end_time - start_time

# Perform the queries and record the times
for size in transaction_sizes:
    sql_time = sqlite_query(size)
    nosql_time = mongodb_query(size)
    sql_times.append(sql_time)
    nosql_times.append(nosql_time)
    print(f"Transaction Size: {size} - SQLite Time: {sql_time:.4f} s, MongoDB Time: {nosql_time:.4f} s")

# Close the database connections
sqlite_conn.close()
mongo_client.close()

# Plotting the results
plt.figure(figsize=(10, 6))
plt.bar(np.array(transaction_sizes) - 50, nosql_times, width=100, label='NoSQL (MongoDB)', color='skyblue')
plt.bar(np.array(transaction_sizes) + 50, sql_times, width=100, label='SQL (SQLite)', color='orange')

# Adding labels and title
plt.xlabel('Transactions size')
plt.ylabel('Query Processing time in seconds')
plt.title('Performance comparison of SQL (SQLite) and NoSQL (MongoDB)')
plt.xticks(transaction_sizes)
plt.legend()

# Show the plot
plt.show()
