import sqlite3
from pymongo import MongoClient
from collections import deque

class SchemaMigration:
    def __init__(self, sqlite_db_name, mongo_uri="mongodb://localhost:27017/", mongo_db_name="migrated_db"):
        # SQLite connection setup
        self.sqlite_db_name = sqlite_db_name
        self.conn = sqlite3.connect(self.sqlite_db_name)
        self.cursor = self.conn.cursor()
        
        # MongoDB connection setup
        self.client = MongoClient(mongo_uri)
        self.mongo_db = self.client[mongo_db_name]
        
    def get_tables(self):
        """Get all table names from SQLite"""
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [table[0] for table in self.cursor.fetchall()]

    def get_foreign_keys(self, table_name):
        """Get foreign keys for a given table"""
        self.cursor.execute(f"PRAGMA foreign_key_list({table_name});")
        return self.cursor.fetchall()

    def migrate_schema(self):
        """Apply schema migration according to execution flow"""
        all_tables = self.get_tables()
        migrated_tables = set()
        linked_tables = deque()  # Linked list to store migration order

        # Start with the first table for schema evaluation
        for table_name in all_tables:
            if table_name in migrated_tables:
                continue
            self.evaluate_table(table_name, migrated_tables, linked_tables)

        print("Linked tables for migration:", list(linked_tables))
        return list(linked_tables)

    def evaluate_table(self, table_name, migrated_tables, linked_tables):
        """Evaluate SQL schema and add related tables based on foreign keys"""
        visited = set()  # Track visited tables to avoid cycles
        queue = deque([table_name])
        
        while queue:
            current_table = queue.popleft()
            if current_table in visited:
                continue

            visited.add(current_table)
            linked_tables.append(current_table)
            migrated_tables.add(current_table)
            
            # Get foreign keys to find related tables
            foreign_keys = self.get_foreign_keys(current_table)
            for fk in foreign_keys:
                related_table = fk[2]  # FK points to related table
                
                # Add related table to linked list if not yet migrated
                if related_table not in migrated_tables:
                    queue.append(related_table)

    def rename_columns_with_table_name(self, table_name, columns):
        """Rename columns by prefixing with table name"""
        return [f"{table_name}_{col}" if col != 'id' else col for col in columns]

    def migrate_data(self, linked_tables):
        """Migrate data from SQLite tables to MongoDB based on migration order"""
        for table_name in linked_tables:
            # Get columns and prepare data for migration
            self.cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [col[1] for col in self.cursor.fetchall()]
            renamed_columns = self.rename_columns_with_table_name(table_name, columns)

            self.cursor.execute(f"SELECT * FROM {table_name};")
            rows = self.cursor.fetchall()

            # Build MongoDB documents
            mongo_data = []
            for row in rows:
                document = {col_name: value for col_name, value in zip(renamed_columns, row)}
                mongo_data.append(document)

            # Insert data into MongoDB
            mongo_collection = self.mongo_db[table_name]
            if mongo_data:
                mongo_collection.insert_many(mongo_data)
                print(f"Data from table '{table_name}' migrated to MongoDB.")

        # Print migrated data from MongoDB
        #print("\nMigrated Data Verification:")
        #for table_name in linked_tables:
            #mongo_collection = self.mongo_db[table_name]
            #print(f"\nData in MongoDB for '{table_name}':")
            #for doc in mongo_collection.find():
                #print(doc)

    def close(self):
        """Close SQLite connection"""
        self.conn.close()

# Usage

sqlite_db_name = 'example.db'  # Your SQLite database file name
#client = MongoClient('mongodb://localhost:27017', connectTimeoutMS=300000, socketTimeoutMS=300000)
migration = SchemaMigration(sqlite_db_name)

# Migrate schema and data for all tables in linked order
linked_tables = migration.migrate_schema()
migration.migrate_data(linked_tables)

# Close SQLite connection
migration.close()
