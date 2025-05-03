import os
import sqlite3
import pandas as pd
from pymongo import MongoClient
from typing import Dict, List, Any, Union

class QueryExecutor:
## Class for executing select queries on SQLite or MongoDB databases
    
    def __init__(self, sqlite_db_path="ohlc.db", 
                 mongo_uri="mongodb://localhost:27017", 
                 mongo_db="stock_data", 
                 mongo_collection="ohlc"):
        ##  Initialize database connections.
        #  Args:
        #     sqlite_db_path: Path to the SQLite database file
        #     mongo_uri: MongoDB connection URI
        #     mongo_db: MongoDB database name
        #     mongo_collection: MongoDB collection name
 
        self.sqlite_db_path = sqlite_db_path
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        
        # Connections
        self.sqlite_conn = None
        self.mongo_client = None
    
    def check_db(self, query: Union[str, Dict], db_type: str = "sqlite") -> Dict[str, Any]:
        # Execute a select query on the specified database.
        # Args:
        #     query: SQL query string for SQLite or query dictionary for MongoDB
        #     db_type: Type of database to query ('sqlite' or 'mongodb')
        # Returns:
        #     Dictionary with query results and metadata

        if db_type.lower() == "sqlite":
            results = self._execute_sqlite_query(query)
        elif db_type.lower() == "mongodb":
            results = self._execute_mongodb_query(query)
        else:
            results = [{"error": f"Unsupported database type: {db_type}"}]
        
        return {
            "db_type": db_type,
            "query": query,
            "results": results
        }
    
    def _execute_sqlite_query(self, query: str) -> List[Dict[str, Any]]:
        # Execute a SQL query on the SQLite database.
        # Args:
        #     query: SQL query string 
        # Returns:
        #     List of result dictionaries
   
        try:
            # Open connection if not open
            if not self.sqlite_conn or not hasattr(self.sqlite_conn, 'cursor'):
                if not os.path.exists(self.sqlite_db_path):
                    return [{"error": f"SQLite database {self.sqlite_db_path} does not exist. Run db_ops.py first"}]
                self.sqlite_conn = sqlite3.connect(self.sqlite_db_path)
            
            # Ensure query is a SELECT statement
            if not query.strip().upper().startswith("SELECT"):
                return [{"error": "Only SELECT queries are allowed"}]
            
            # Execute query and fetch results
            df = pd.read_sql_query(query, self.sqlite_conn)
            results = df.to_dict(orient='records')
            
            return results
        except Exception as e:
            return [{"error": f"Error executing SQLite query: {e}"}]
    
    def _execute_mongodb_query(self, query: Dict) -> List[Dict[str, Any]]:
        # Execute a query on the MongoDB database.
        # Args:
        #     query: MongoDB query dictionary with 'filter' and 'options'
        # Returns:
        #     List of result dictionaries

        try:
            # Connect to MongoDB
            if not self.mongo_client:
                self.mongo_client = MongoClient(self.mongo_uri)
                self.mongo_client.server_info()
            
            # Get database and collection
            db = self.mongo_client[self.mongo_db]
            collection = db[self.mongo_collection]
            
            # Extract filter and options
            query_filter = query.get("filter", {})
            query_options = query.get("options", {})
            
            # Extract options
            sort_option = query_options.get("sort", None)
            limit_option = query_options.get("limit", None)
            project_option = query_options.get("projection", None)
            
            cursor = collection.find(query_filter, project_option) if project_option else collection.find(query_filter)
            
            if sort_option:
                cursor = cursor.sort(sort_option)
            
            if limit_option:
                cursor = cursor.limit(limit_option)
            
            # Convert cursor to list 
            results = []
            for doc in cursor:
                # Convert ObjectId to string for JSON serialization
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                results.append(doc)
            
            return results
        except Exception as e:
            return [{"error": f"Error executing MongoDB query: {e}"}]
    
    def close(self):
        # Close all database connections
        if self.sqlite_conn:
            self.sqlite_conn.close()
            self.sqlite_conn = None
        
        if self.mongo_client:
            self.mongo_client.close()
            self.mongo_client = None

if __name__ == "__main__":
    executor = QueryExecutor()
    
    # Example SQL query
    sql_query = "SELECT date, open, close FROM ohlc ORDER BY date DESC LIMIT 5"
    sql_result = executor.check_db(sql_query, "sqlite")
    print("\nSQL Query:")
    print(f"Query: {sql_result['query']}")
    print(f"Results: {sql_result['results']}")
    
    # Example MongoDB query
    mongo_query = {
        "filter": {},
        "options": {
            "sort": [("date", -1)],
            "limit": 5,
            "projection": {"date": 1, "open": 1, "close": 1, "_id": 0}
        }
    }
    mongo_result = executor.check_db(mongo_query, "mongodb")
    print("\nMongoDB Query Example:")
    print(f"Query: {mongo_result['query']}")
    print(f"Results: {mongo_result['results']}")
    
    executor.close()