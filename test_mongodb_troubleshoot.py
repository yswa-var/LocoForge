#!/usr/bin/env python3
"""
MongoDB Atlas Connection Troubleshooting Script
"""

import os
import getpass
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Load environment variables
load_dotenv()

def test_connection(connection_string):
    """Test a MongoDB connection string"""
    try:
        print(f"🔗 Testing connection...")
        client = MongoClient(connection_string, serverSelectionTimeoutMS=10000)
        client.admin.command("ping")
        print("✅ Connection successful!")
        
        # Test database access
        db = client.grocery_warehouse
        collections = db.list_collection_names()
        print(f"📊 Available collections: {collections}")
        
        client.close()
        return True
    except ConnectionFailure as e:
        print(f"❌ Connection failed: {e}")
        return False
    except OperationFailure as e:
        print(f"❌ Authentication failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Main troubleshooting function"""
    print("🔍 MongoDB Atlas Connection Troubleshooting")
    print("=" * 50)
    
    print("\n📋 To fix your MongoDB Atlas connection, you need to:")
    print("1. Go to your MongoDB Atlas dashboard")
    print("2. Get the correct connection string")
    print("3. Update your .env file")
    
    print("\n🔧 Let's test your current connection first...")
    
    # Test current connection from .env
    current_connection = os.getenv("MONGO_DB")
    if current_connection:
        print(f"Current connection string: {current_connection[:50]}...")
        if test_connection(current_connection):
            print("🎉 Your current connection works!")
            return
    else:
        print("❌ No MONGO_DB found in .env file")
    
    print("\n❌ Current connection failed. Let's get the correct credentials...")
    
    print("\n📝 Please provide your MongoDB Atlas credentials:")
    print("(You can find these in your MongoDB Atlas dashboard)")
    
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    
    if not username or not password:
        print("❌ Username and password are required")
        return
    
    # Test with new credentials
    base_url = "cluster0.ku0y7rt.mongodb.net"
    test_connection_string = f"mongodb+srv://{username}:{password}@{base_url}/?retryWrites=true&w=majority&appName=Cluster0"
    
    print(f"\n🔗 Testing new credentials...")
    if test_connection(test_connection_string):
        print(f"\n🎉 SUCCESS! Working connection string:")
        print(f"MONGO_DB={test_connection_string}")
        print(f"\nPlease update your .env file with this connection string.")
    else:
        print("\n❌ Connection still failed with new credentials.")
        print("\n🔧 Additional troubleshooting steps:")
        print("1. Check if your IP is whitelisted in MongoDB Atlas")
        print("2. Verify your MongoDB Atlas cluster is running")
        print("3. Try resetting your password in MongoDB Atlas")
        print("4. Check if you're using the correct cluster URL")

if __name__ == "__main__":
    main()
