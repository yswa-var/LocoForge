#!/usr/bin/env python3
"""
mongo_db_ops.py - MongoDB manager for Apple stock data

This script manages a MongoDB database to store and query Apple (AAPL) stock data.
It checks for database/collection existence, downloads data if needed using yfinance,
and performs queries on the stored documents.
"""

import pymongo
import yfinance as yf
import pandas as pd
from datetime import datetime

# MongoDB connection parameters
mongo_uri = "mongodb://localhost:27017"
db_name = "stock_data"
collection_name = "ohlc"

# Function to check if database and collection exist
def check_collection_exists(client, db_name, collection_name):
    try:
        db = client[db_name]
        collection_exists = collection_name in db.list_collection_names()
        
        if not collection_exists:
            return False, 0
        
        # Check if collection has data
        collection = db[collection_name]
        row_count = collection.count_documents({})
        
        return collection_exists, row_count
    except Exception as e:
        print(f"Error checking collection: {e}")
        return False, 0

# Function to create and populate ohlc collection
def insert_apple_stock_data(client, db_name, collection_name):
    try:
        # Download Apple stock data using yfinance
        ticker = "AAPL"
        start_date = "2020-01-01"
        end_date = datetime.today().strftime("%Y-%m-%d")
        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)
        
        # Flatten multi-level columns and select AAPL data
        df = df.xs("AAPL", level="Ticker", axis=1, drop_level=True)
        
        # Reset index to make 'Date' a column
        df = df.reset_index()
        
        # Debug: Print first few rows to inspect data
        print("Sample data from yfinance:")
        print(df.head())
        
        # Prepare documents for MongoDB
        db = client[db_name]
        collection = db[collection_name]
        
        documents = []
        for _, row in df.iterrows():
            volume = int(row["Volume"].iloc[0]) if isinstance(row["Volume"], pd.Series) else int(row["Volume"])
            date_str = row["Date"].strftime("%Y-%m-%d") if isinstance(row["Date"], pd.Timestamp) else str(row["Date"])
            document = {
                "date": date_str,
                "open": float(row["Open"].iloc[0]) if isinstance(row["Open"], pd.Series) else float(row["Open"]),
                "high": float(row["High"].iloc[0]) if isinstance(row["High"], pd.Series) else float(row["High"]),
                "low": float(row["Low"].iloc[0]) if isinstance(row["Low"], pd.Series) else float(row["Low"]),
                "close": float(row["Close"].iloc[0]) if isinstance(row["Close"], pd.Series) else float(row["Close"]),
                "volume": volume
            }
            documents.append(document)
        
        # Insert documents in bulk, skipping duplicates
        if documents:
            collection.insert_many(documents, ordered=False)
        
        print("Apple stock data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")

# Function to perform query operations
def select_ohlc_data(client, db_name, collection_name):
    try:
        db = client[db_name]
        collection = db[collection_name]
        
        # Query: Get the latest 5 records, sorted by date descending
        cursor = collection.find().sort("date", -1).limit(5)
        
        print("\nLatest 5 OHLC records:")
        for doc in cursor:
            print(f"Date: {doc['date']}, Open: {doc['open']}, High: {doc['high']}, "
                  f"Low: {doc['low']}, Close: {doc['close']}, Volume: {doc['volume']}")
    except Exception as e:
        print(f"Error selecting data: {e}")

# Main logic
try:
    # Connect to MongoDB
    client = pymongo.MongoClient(mongo_uri)
    
    if client.server_info():  # Check if MongoDB is running
        print("Connected to MongoDB.")
        
        # Check if collection exists and has data
        collection_exists, row_count = check_collection_exists(client, db_name, collection_name)
        
        if collection_exists and row_count > 0:
            print(f"Collection {collection_name} exists with {row_count} records. Querying data.")
            select_ohlc_data(client, db_name, collection_name)
        else:
            print(f"Collection {collection_name} is missing or empty. Creating and populating it.")
            insert_apple_stock_data(client, db_name, collection_name)
            select_ohlc_data(client, db_name, collection_name)
    
    client.close()
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")