#!/usr/bin/env python3
"""
Test script for NoSQL database setup with Faker data generation
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_faker_import():
    """Test that Faker can be imported"""
    try:
        from faker import Faker
        fake = Faker()
        print("✅ Faker library imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Faker import failed: {e}")
        return False

def test_mongoengine_import():
    """Test that MongoEngine can be imported"""
    try:
        from mongoengine import connect, Document, StringField, IntField, ListField, DateTimeField, EmbeddedDocumentField, EmbeddedDocument, BooleanField, DecimalField
        print("✅ MongoEngine library imported successfully")
        return True
    except ImportError as e:
        print(f"❌ MongoEngine import failed: {e}")
        return False

def test_setup_import():
    """Test that the setup module can be imported"""
    try:
        from my_agent.utils.setup_nosql_db import NoSQLDatabaseSetup
        print("✅ NoSQLDatabaseSetup imported successfully")
        return True
    except ImportError as e:
        print(f"❌ NoSQLDatabaseSetup import failed: {e}")
        return False

def test_environment_variables():
    """Test that required environment variables are set"""
    print("\n🔍 Checking environment variables...")
    
    mongo_db = os.getenv("MONGO_DB")
    if mongo_db:
        print(f"✅ MONGO_DB: SET")
    else:
        print(f"❌ MONGO_DB: NOT SET")
        return False
    
    return True

def test_database_connection():
    """Test database connection"""
    print("\n🔍 Testing database connection...")
    
    try:
        from my_agent.utils.setup_nosql_db import NoSQLDatabaseSetup
        
        setup = NoSQLDatabaseSetup()
        print("✅ Database connection successful")
        
        # Test getting stats
        stats = setup.get_database_stats()
        print(f"📊 Current database stats: {stats}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_data_generation():
    """Test fake data generation"""
    print("\n🔍 Testing fake data generation...")
    
    try:
        from my_agent.utils.setup_nosql_db import NoSQLDatabaseSetup
        
        setup = NoSQLDatabaseSetup()
        
        # Test generating a single sale
        sale = setup.generate_sale()
        print("✅ Sale generation successful")
        print(f"   Customer: {sale.customer.email} (Age: {sale.customer.age})")
        print(f"   Items: {len(sale.items)} items")
        print(f"   Store: {sale.storeLocation}")
        print(f"   Method: {sale.purchaseMethod}")
        
        # Test generating items
        item = setup.generate_item()
        print("✅ Item generation successful")
        print(f"   Name: {item.name}")
        print(f"   Price: ${item.price}")
        print(f"   Tags: {item.tags}")
        
        return True
        
    except Exception as e:
        print(f"❌ Data generation failed: {e}")
        return False

def test_small_population():
    """Test populating database with a small amount of data"""
    print("\n🔍 Testing small data population...")
    
    try:
        from my_agent.utils.setup_nosql_db import NoSQLDatabaseSetup
        
        setup = NoSQLDatabaseSetup()
        
        # Generate 10 test sales
        num_sales = 10
        print(f"🔄 Generating {num_sales} test sales...")
        
        sales_list = []
        for i in range(num_sales):
            sale = setup.generate_sale()
            sales_list.append(sale)
            if (i + 1) % 5 == 0:
                print(f"   Generated {i + 1} sales...")
        
        # Insert the sales
        from my_agent.utils.setup_nosql_db import Sale
        Sale.objects.insert(sales_list)
        
        print(f"✅ Successfully inserted {len(sales_list)} test sales")
        
        # Verify the data
        total_sales = Sale.objects.count()
        print(f"📊 Total sales in database: {total_sales}")
        
        return True
        
    except Exception as e:
        print(f"❌ Small population test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 NoSQL Database Setup Test")
    print("=" * 50)
    
    # Test imports
    if not test_faker_import():
        print("\n❌ Faker import test failed")
        return
    
    if not test_mongoengine_import():
        print("\n❌ MongoEngine import test failed")
        return
    
    if not test_setup_import():
        print("\n❌ Setup import test failed")
        return
    
    # Test environment
    if not test_environment_variables():
        print("\n❌ Environment variables test failed")
        return
    
    # Test database connection
    if not test_database_connection():
        print("\n❌ Database connection test failed")
        return
    
    # Test data generation
    if not test_data_generation():
        print("\n❌ Data generation test failed")
        return
    
    # Test small population
    if not test_small_population():
        print("\n❌ Small population test failed")
        return
    
    print("\n" + "=" * 50)
    print("🎉 All tests passed!")
    print("✅ NoSQL database setup is working correctly")
    print("✅ Faker data generation is functional")
    print("✅ Database connection is stable")
    print("✅ Data insertion is working")
    
    print("\n🚀 Ready to populate the database with fake data!")
    print("   Run: python my_agent/utils/setup_nosql_db.py")

if __name__ == "__main__":
    main()
