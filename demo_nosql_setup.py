#!/usr/bin/env python3
"""
Demonstration of NoSQL Database Setup with Faker Data Generation
Shows how to create and populate the sample_supplies database
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def demonstrate_data_structure():
    """Demonstrate the data structure and schema"""
    print("🔍 Sample Supplies Database Structure")
    print("=" * 50)
    
    print("📊 Database: sample_supplies")
    print("📋 Collection: sales")
    print()
    
    print("🗂️  Document Structure:")
    print("   Sale Document:")
    print("   ├── couponUsed: Boolean")
    print("   ├── customer: EmbeddedDocument")
    print("   │   ├── age: Integer (18-80)")
    print("   │   ├── email: String (fake email)")
    print("   │   ├── gender: String (M/F)")
    print("   │   └── satisfaction: Integer (1-5)")
    print("   ├── items: List[EmbeddedDocument]")
    print("   │   ├── name: String (product name)")
    print("   │   ├── price: Decimal128 (realistic price)")
    print("   │   ├── quantity: Integer (1-10)")
    print("   │   └── tags: List[String] (category tags)")
    print("   ├── purchaseMethod: String (Online/In store/Phone/Mobile app)")
    print("   ├── saleDate: DateTime (last 2 years)")
    print("   └── storeLocation: String (20 major cities)")
    print()

def demonstrate_product_categories():
    """Demonstrate the product categories"""
    print("🛍️  Product Categories")
    print("=" * 50)
    
    categories = {
        "Office Supplies": {
            "items": ["Pencil", "Pen", "Notebook", "Stapler", "Paper clips", "Tape", "Scissors", "Ruler", "Calculator", "Desk organizer"],
            "tags": ["office", "stationery", "desk", "work"],
            "price_range": "$2-50"
        },
        "Electronics": {
            "items": ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "USB cable", "Power adapter", "Webcam", "Microphone", "Tablet"],
            "tags": ["electronics", "tech", "computer", "digital"],
            "price_range": "$50-2000"
        },
        "Home & Garden": {
            "items": ["Garden hose", "Plant pot", "Seeds", "Fertilizer", "Garden tools", "Outdoor lights", "Bird feeder", "Watering can", "Pruning shears", "Mulch"],
            "tags": ["garden", "outdoor", "plants", "home"],
            "price_range": "$20-500"
        },
        "Kitchen & Dining": {
            "items": ["Coffee maker", "Toaster", "Blender", "Microwave", "Dishware", "Utensils", "Cookware", "Food storage", "Spice rack", "Wine glasses"],
            "tags": ["kitchen", "cooking", "dining", "food"],
            "price_range": "$15-300"
        },
        "Sports & Outdoors": {
            "items": ["Yoga mat", "Dumbbells", "Running shoes", "Water bottle", "Backpack", "Tent", "Sleeping bag", "Camping stove", "Hiking boots", "Bicycle"],
            "tags": ["sports", "fitness", "outdoor", "exercise"],
            "price_range": "$25-400"
        }
    }
    
    for category, data in categories.items():
        print(f"📦 {category}")
        print(f"   Items: {len(data['items'])} products")
        print(f"   Tags: {', '.join(data['tags'])}")
        print(f"   Price Range: {data['price_range']}")
        print(f"   Sample Items: {', '.join(data['items'][:3])}...")
        print()

def demonstrate_store_locations():
    """Demonstrate the store locations"""
    print("🏪 Store Locations")
    print("=" * 50)
    
    locations = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
        "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
        "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington"
    ]
    
    print(f"📍 {len(locations)} Major Cities:")
    for i, location in enumerate(locations, 1):
        print(f"   {i:2d}. {location}")
    print()

def demonstrate_purchase_methods():
    """Demonstrate the purchase methods"""
    print("💳 Purchase Methods")
    print("=" * 50)
    
    methods = ["Online", "In store", "Phone", "Mobile app"]
    
    print("📱 Available Purchase Methods:")
    for method in methods:
        print(f"   • {method}")
    print()

def demonstrate_sample_data():
    """Demonstrate sample data generation"""
    print("🎲 Sample Data Generation")
    print("=" * 50)
    
    try:
        from my_agent.utils.setup_nosql_db import NoSQLDatabaseSetup
        
        # Create setup instance (without connecting to database)
        setup = NoSQLDatabaseSetup()
        
        print("📊 Sample Generated Data:")
        print()
        
        # Generate a few sample sales
        for i in range(3):
            sale = setup.generate_sale()
            print(f"🛒 Sale #{i+1}:")
            print(f"   Customer: {sale.customer.email} ({sale.customer.age} years old, {sale.customer.gender})")
            print(f"   Satisfaction: {sale.customer.satisfaction}/5")
            print(f"   Store: {sale.storeLocation}")
            print(f"   Method: {sale.purchaseMethod}")
            print(f"   Coupon Used: {'Yes' if sale.couponUsed else 'No'}")
            print(f"   Items ({len(sale.items)}):")
            for j, item in enumerate(sale.items, 1):
                print(f"     {j}. {item.name} - ${item.price} x{item.quantity} ({', '.join(item.tags)})")
            print(f"   Date: {sale.saleDate.strftime('%Y-%m-%d')}")
            print()
        
    except Exception as e:
        print(f"⚠️  Could not generate sample data: {e}")
        print("   (This is expected if MongoDB is not running)")

def demonstrate_usage_instructions():
    """Demonstrate how to use the setup"""
    print("🚀 Usage Instructions")
    print("=" * 50)
    
    print("📋 To set up the NoSQL database:")
    print()
    print("1. Ensure MongoDB is running:")
    print("   brew services start mongodb-community")
    print()
    print("2. Set up environment variables in .env:")
    print("   MONGO_DB=mongodb://localhost:27017/")
    print("   OPENAPI_KEY=your_openai_api_key_here")
    print()
    print("3. Run the setup script:")
    print("   python my_agent/utils/setup_nosql_db.py")
    print()
    print("4. Choose from the options:")
    print("   1. Clear existing data and populate with new data")
    print("   2. Add to existing data")
    print("   3. Generate seasonal data")
    print("   4. Show database statistics")
    print("   5. Create indexes only")
    print()
    print("5. Test the setup:")
    print("   python test_nosql_setup.py")
    print()

def demonstrate_query_examples():
    """Demonstrate example queries"""
    print("🔍 Example Queries")
    print("=" * 50)
    
    examples = [
        "Show all sales with coupons used",
        "Find sales with high customer satisfaction (4 or 5)",
        "Show online purchases",
        "List sales by store location",
        "Find sales with specific items like pencils",
        "Show sales by customer age groups",
        "List total sales by item",
        "Find sales with specific tags",
        "Show average satisfaction by gender",
        "Find sales from specific date ranges",
        "List sales by purchase method",
        "Show sales with high-value items",
        "Find sales by customer email domain",
        "Show sales with multiple items",
        "List sales by customer satisfaction level"
    ]
    
    print("📝 Sample queries you can run:")
    for i, query in enumerate(examples, 1):
        print(f"   {i:2d}. {query}")
    print()

def main():
    """Run the demonstration"""
    print("🛒 NoSQL Database Setup Demonstration")
    print("=" * 60)
    
    # Demonstrate data structure
    demonstrate_data_structure()
    
    # Demonstrate product categories
    demonstrate_product_categories()
    
    # Demonstrate store locations
    demonstrate_store_locations()
    
    # Demonstrate purchase methods
    demonstrate_purchase_methods()
    
    # Demonstrate sample data
    demonstrate_sample_data()
    
    # Demonstrate usage instructions
    demonstrate_usage_instructions()
    
    # Demonstrate query examples
    demonstrate_query_examples()
    
    print("=" * 60)
    print("🎉 Setup Features Summary:")
    print("✅ Realistic fake data generation using Faker")
    print("✅ 5 product categories with 50+ items")
    print("✅ 20 major US cities as store locations")
    print("✅ 4 different purchase methods")
    print("✅ Seasonal data patterns")
    print("✅ Customer demographics (age, gender, satisfaction)")
    print("✅ Realistic pricing and quantities")
    print("✅ Product categorization with tags")
    print("✅ Database indexing for performance")
    print("✅ Batch processing for large datasets")
    
    print("\n🚀 Ready to create your sample_supplies database!")

if __name__ == "__main__":
    main()
