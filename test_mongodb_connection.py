#!/usr/bin/env python3
"""
Simple MongoDB Atlas connection test
"""

import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Load environment variables
load_dotenv()

def test_mongodb_connection():
    """Test MongoDB Atlas connection"""
    
    # Get connection string from environment
    connection_string = os.getenv("MONGO_DB")
    
    if not connection_string:
        print("❌ Error: MONGO_DB not found in environment variables")
        return False
    
    print(f"🔗 Testing connection to MongoDB Atlas...")
    print(f"Connection string: {connection_string[:50]}...")  # Show first 50 chars for privacy
    
    try:
        # Method 1: Standard MongoDB Atlas connection
        print("\n📡 Method 1: Standard connection with ServerApi...")
        client = MongoClient(connection_string, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("✅ Successfully connected with Method 1!")
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Method 1 failed: {e}")
        
        try:
            # Method 2: Simple connection
            print("\n📡 Method 2: Simple connection...")
            client = MongoClient(connection_string)
            client.admin.command('ping')
            print("✅ Successfully connected with Method 2!")
            client.close()
            return True
            
        except Exception as e2:
            print(f"❌ Method 2 failed: {e2}")
            
            try:
                # Method 3: Connection with timeout
                print("\n📡 Method 3: Connection with timeout...")
                client = MongoClient(
                    connection_string,
                    server_api=ServerApi('1'),
                    connectTimeoutMS=60000,
                    socketTimeoutMS=60000
                )
                client.admin.command('ping')
                print("✅ Successfully connected with Method 3!")
                client.close()
                return True
                
            except Exception as e3:
                print(f"❌ Method 3 failed: {e3}")
                return False

if __name__ == "__main__":
    success = test_mongodb_connection()
    if success:
        print("\n🎉 MongoDB Atlas connection test successful!")
    else:
        print("\n💥 All connection methods failed!")
        print("\n🔧 Troubleshooting tips:")
        print("1. Check if your MongoDB Atlas cluster is running")
        print("2. Verify the password in your .env file")
        print("3. Check if your IP address is whitelisted in MongoDB Atlas")
        print("4. Ensure your MongoDB Atlas cluster is accessible") 