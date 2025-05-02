#!/usr/bin/env python3
"""
db_ops.py - SQLite database manager for Apple stock data

This script manages a local SQLite database to store and query Apple (AAPL) stock data.
It checks for database existence, creates tables if needed, downloads data from yfinance,
and performs queries on the stored data.
"""

import os
import sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime

def check_db_exists(db_name="ohlc.db"):
    """Check if the database file exists in the current directory."""
    return os.path.exists(db_name)

def check_table_exists(cursor, table_name="ohlc"):
    """Check if a specific table exists in the database."""
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    return cursor.fetchone() is not None

def check_table_has_data(cursor, table_name="ohlc"):
    """Check if a table has any records."""
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    return count > 0

def create_ohlc_table(cursor):
    """Create the OHLC table with appropriate columns and constraints."""
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ohlc (
        date TEXT PRIMARY KEY,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER
    )
    ''')

def download_aapl_data():
    """Download Apple stock data from January 1, 2020 to current date."""
    start_date = "2020-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Downloading AAPL data from {start_date} to {end_date}...")
    
    # Download data using yfinance
    aapl = yf.Ticker("AAPL")
    data = aapl.history(start=start_date, end=end_date)
    
    # Reset index to make Date a column
    data = data.reset_index()
    
    # Debug: Show sample of downloaded data
    print("\nSample of downloaded data:")
    print(data.head())
    print(f"Downloaded {len(data)} records.")
    
    return data

def insert_data_to_db(conn, data):
    """Insert downloaded data into the OHLC table."""
    cursor = conn.cursor()
    
    # Counter for tracking inserted records
    inserted_count = 0
    
    print("\nInserting data into database...")
    
    for _, row in data.iterrows():
        # Convert date to string in YYYY-MM-DD format
        date_str = row['Date'].strftime('%Y-%m-%d')
        
        # Handle potential Series objects by converting to scalar values
        try:
            open_val = float(row['Open'])
            high_val = float(row['High'])
            low_val = float(row['Low'])
            close_val = float(row['Close'])
            
            # Volume might be a Series in some yfinance versions
            if isinstance(row['Volume'], pd.Series):
                volume_val = int(row['Volume'].iloc[0])
            else:
                volume_val = int(row['Volume'])
                
            # Use INSERT OR IGNORE to skip duplicates
            cursor.execute('''
            INSERT OR IGNORE INTO ohlc (date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (date_str, open_val, high_val, low_val, close_val, volume_val))
            
            inserted_count += 1
            
        except Exception as e:
            print(f"Error inserting row for date {date_str}: {e}")
            print(f"Row data: {row}")
    
    # Commit the changes
    conn.commit()
    print(f"Successfully inserted {inserted_count} records into the database.")

def query_latest_records(cursor, limit=5):
    """Query and display the latest records from the OHLC table."""
    cursor.execute('''
    SELECT date, open, high, low, close, volume 
    FROM ohlc 
    ORDER BY date DESC 
    LIMIT ?
    ''', (limit,))
    
    records = cursor.fetchall()
    
    print(f"\nLatest {limit} records from OHLC table:")
    print("Date        | Open     | High     | Low      | Close    | Volume")
    print("-" * 70)
    
    for record in records:
        date, open_val, high_val, low_val, close_val, volume = record
        print(f"{date} | {open_val:.2f} | {high_val:.2f} | {low_val:.2f} | {close_val:.2f} | {volume:,}")
    
    return records

def main():
    """Main function to orchestrate database operations."""
    db_name = "ohlc.db"
    db_exists = check_db_exists(db_name)
    
    if db_exists:
        print(f"Database '{db_name}' found.")
    else:
        print(f"Database '{db_name}' not found. Creating new database.")
    
    # Connect to the database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Check if OHLC table exists
    table_exists = check_table_exists(cursor)
    
    if table_exists:
        print("OHLC table exists.")
        
        # Check if the table has data
        has_data = check_table_has_data(cursor)
        if has_data:
            print("OHLC table contains data.")
        else:
            print("OHLC table is empty. Will populate with data.")
            
            # Download and insert Apple stock data
            data = download_aapl_data()
            insert_data_to_db(conn, data)
    else:
        print("OHLC table does not exist. Creating table.")
        create_ohlc_table(cursor)
        
        # Download and insert Apple stock data
        data = download_aapl_data()
        insert_data_to_db(conn, data)
    
    # Query and display the latest records
    query_latest_records(cursor)
    
    # Close the connection
    conn.close()
    print("\nDatabase operations completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")