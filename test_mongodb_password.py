#!/usr/bin/env python3
"""
Test MongoDB Atlas connection with password prompt
"""

import os
import getpass
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Load environment variables with override to ensure latest values
load_dotenv(override=True)

def test_mongodb_with_password():
    """Test MongoDB Atlas connection with password prompt"""
    
    # Get connection string from environment
    connection_string = os.getenv("MONGO_DB")
    
    if not connection_string:
        print("❌ Error: MONGO_DB not found in environment variables")
        return False
    
    print(f"🔗 Testing connection to MongoDB Atlas...")
    print(f"Connection string: {connection_string[:50]}...")  # Show first 50 chars for privacy
    
    # Check if password is still a placeholder
    if "<db_password>" in connection_string:
        print("\n⚠️  Warning: Password placeholder detected!")
        print("Please update your .env file with the actual password.")
        print("Current format: mongodb+srv://anton:<db_password>@cluster0...")
        print("Should be: mongodb+srv://anton:YOUR_ACTUAL_PASSWORD@cluster0...")
        return False
    
    try:
        # Test connection
        print("\n📡 Testing connection...")
        client = MongoClient(connection_string, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB Atlas!")
        
        # Test database access
        db = client.grocery_warehouse
        collections = db.list_collection_names()
        print(f"📊 Available collections: {collections}")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n🔧 Common issues:")
        print("1. Incorrect password in .env file")
        print("2. IP address not whitelisted in MongoDB Atlas")
        print("3. MongoDB Atlas cluster is down or inaccessible")
        print("4. Network connectivity issues")
        return False

if __name__ == "__main__":
    success = test_mongodb_with_password()
    if success:
        print("\n🎉 MongoDB Atlas connection test successful!")
        print("You can now run the NoSQL agent!")
    else:
        print("\n💥 Connection test failed!")
        print("Please fix the issues above and try again.")

# Additional test with a minimal script
uri = "mongodb+srv://anton:uqCFB5ZHSFzKhoUk@cluster0.ku0y7rt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))
try:
    client.admin.command('ping')
    print("✅ Successfully connected to MongoDB Atlas!")
except Exception as e:
    print("❌ Connection failed:", e) 