#!/usr/bin/env python3
"""
MongoDB Grocery Warehouse Database Setup Script
This script automates the complete setup process for the grocery warehouse database.
"""

import os
import sys
import subprocess
from check_mongodb import check_mongodb_connection, check_python_dependencies

def install_dependencies():
    """Install required Python packages."""
    print("📦 Installing Python dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def create_database():
    """Create the grocery warehouse database."""
    print("🗄️  Creating grocery warehouse database...")
    try:
        from nosql_db_init import main as init_main
        init_main()
        print("✅ Database created successfully")
        return True
    except Exception as e:
        print(f"❌ Failed to create database: {e}")
        return False

def test_database():
    """Test the database with complex queries."""
    print("🧪 Testing database with complex queries...")
    try:
        from test_queries import main as test_main
        test_main()
        print("✅ Database tests completed successfully")
        return True
    except Exception as e:
        print(f"❌ Database tests failed: {e}")
        return False

def main():
    """Main setup function."""
    print("🚀 MONGODB GROCERY WAREHOUSE DATABASE SETUP")
    print("=" * 50)
    
    # Step 1: Check Python dependencies
    print("\n1️⃣  Checking Python dependencies...")
    if not check_python_dependencies():
        print("Installing dependencies...")
        if not install_dependencies():
            print("❌ Setup failed: Could not install dependencies")
            return False
    else:
        print("✅ Dependencies are already installed")
    
    # Step 2: Check MongoDB connection
    print("\n2️⃣  Checking MongoDB connection...")
    if not check_mongodb_connection():
        print("❌ Setup failed: MongoDB is not running")
        print("Please start MongoDB and run this script again")
        return False
    else:
        print("✅ MongoDB is running")
    
    # Step 3: Create database
    print("\n3️⃣  Creating database...")
    if not create_database():
        print("❌ Setup failed: Could not create database")
        return False
    
    # Step 4: Test database
    print("\n4️⃣  Testing database...")
    if not test_database():
        print("❌ Setup failed: Database tests failed")
        return False
    
    # Success
    print("\n" + "=" * 50)
    print("🎉 SETUP COMPLETED SUCCESSFULLY!")
    print("=" * 50)
    print("\nYour MongoDB grocery warehouse database is ready!")
    print("\n📊 Database Statistics:")
    print("   - Database: grocery_warehouse")
    print("   - Collections: products, inventory, orders")
    print("   - Total Documents: 13")
    print("\n🔧 Available Scripts:")
    print("   - python nosql_db_init.py    # Recreate database")
    print("   - python test_queries.py     # Run complex queries")
    print("   - python check_mongodb.py    # Check setup status")
    print("\n📚 Documentation:")
    print("   - README.md                  # Complete documentation")
    print("\nThe database is now ready for your NoSQL agent operations!")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n❌ Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error during setup: {e}")
        sys.exit(1) 