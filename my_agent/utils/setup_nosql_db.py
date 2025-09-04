#!/usr/bin/env python3
"""
NoSQL Database Setup with Faker Data Generation
Creates and populates the sample_supplies database with realistic fake data
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dotenv import load_dotenv
from faker import Faker
from mongoengine import connect, Document, StringField, IntField, ListField, DateTimeField, ReferenceField, EmbeddedDocumentField, EmbeddedDocument, FloatField, DictField, BooleanField, DecimalField
import random
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()

# MongoEngine Document Models (matching nosql_agent.py)
class Customer(EmbeddedDocument):
    """Embedded document for customer information"""
    age = IntField()
    email = StringField()
    gender = StringField()
    satisfaction = IntField()


class Item(EmbeddedDocument):
    """Embedded document for item information"""
    name = StringField()
    price = DecimalField()
    quantity = IntField()
    tags = ListField(StringField())


class Sale(Document):
    """Sale document model for sample_supplies"""
    couponUsed = BooleanField()
    customer = EmbeddedDocumentField(Customer)
    items = ListField(EmbeddedDocumentField(Item))
    purchaseMethod = StringField()
    saleDate = DateTimeField()
    storeLocation = StringField()

    meta = {
        'collection': 'sales',
        'allow_inheritance': True
    }


class NoSQLDatabaseSetup:
    """Setup class for NoSQL database with fake data generation"""
    
    def __init__(self, connection_string: str = None):
        """Initialize the database setup"""
        self.connection_string = connection_string or os.getenv("MONGO_DB")
        if not self.connection_string:
            raise ValueError("MongoDB connection string not found. Set MONGO_DB in .env file")
        
        # Connect to MongoDB
        try:
            db_name = "sample_supplies"
            if "mongodb+srv://" in self.connection_string:
                connect(db=db_name, host=self.connection_string)
            else:
                connect(db=db_name, host=self.connection_string)
            logger.info("✅ Connected to MongoDB successfully")
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            raise e
        
        # Sample data for realistic generation
        self.store_locations = [
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
            "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
            "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington"
        ]
        
        self.purchase_methods = ["Online", "In store", "Phone", "Mobile app"]
        
        self.product_categories = {
            "Office Supplies": {
                "items": [
                    "Pencil", "Pen", "Notebook", "Stapler", "Paper clips",
                    "Tape", "Scissors", "Ruler", "Calculator", "Desk organizer"
                ],
                "tags": ["office", "stationery", "desk", "work"]
            },
            "Electronics": {
                "items": [
                    "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones",
                    "USB cable", "Power adapter", "Webcam", "Microphone", "Tablet"
                ],
                "tags": ["electronics", "tech", "computer", "digital"]
            },
            "Home & Garden": {
                "items": [
                    "Garden hose", "Plant pot", "Seeds", "Fertilizer", "Garden tools",
                    "Outdoor lights", "Bird feeder", "Watering can", "Pruning shears", "Mulch"
                ],
                "tags": ["garden", "outdoor", "plants", "home"]
            },
            "Kitchen & Dining": {
                "items": [
                    "Coffee maker", "Toaster", "Blender", "Microwave", "Dishware",
                    "Utensils", "Cookware", "Food storage", "Spice rack", "Wine glasses"
                ],
                "tags": ["kitchen", "cooking", "dining", "food"]
            },
            "Sports & Outdoors": {
                "items": [
                    "Yoga mat", "Dumbbells", "Running shoes", "Water bottle", "Backpack",
                    "Tent", "Sleeping bag", "Camping stove", "Hiking boots", "Bicycle"
                ],
                "tags": ["sports", "fitness", "outdoor", "exercise"]
            }
        }
    
    def generate_customer(self) -> Customer:
        """Generate a fake customer"""
        return Customer(
            age=random.randint(18, 80),
            email=fake.email(),
            gender=random.choice(["M", "F"]),
            satisfaction=random.randint(1, 5)
        )
    
    def generate_item(self) -> Item:
        """Generate a fake item"""
        # Select random category and item
        category = random.choice(list(self.product_categories.keys()))
        category_data = self.product_categories[category]
        item_name = random.choice(category_data["items"])
        
        # Generate realistic price based on category
        if category == "Electronics":
            base_price = random.uniform(50, 2000)
        elif category == "Home & Garden":
            base_price = random.uniform(20, 500)
        elif category == "Kitchen & Dining":
            base_price = random.uniform(15, 300)
        elif category == "Sports & Outdoors":
            base_price = random.uniform(25, 400)
        else:  # Office Supplies
            base_price = random.uniform(2, 50)
        
        # Add some variation
        price = base_price * random.uniform(0.8, 1.2)
        
        return Item(
            name=item_name,
            price=round(price, 2),
            quantity=random.randint(1, 10),
            tags=random.sample(category_data["tags"], min(random.randint(1, len(category_data["tags"])), len(category_data["tags"])))
        )
    
    def generate_sale(self, sale_date: datetime = None) -> Sale:
        """Generate a fake sale"""
        if sale_date is None:
            # Generate date within last 2 years
            days_ago = random.randint(0, 730)
            sale_date = datetime.now() - timedelta(days=days_ago)
        
        # Generate 1-5 items per sale
        num_items = random.randint(1, 5)
        items = [self.generate_item() for _ in range(num_items)]
        
        return Sale(
            couponUsed=random.choices([True, False], weights=[0.3, 0.7])[0],  # 30% use coupons
            customer=self.generate_customer(),
            items=items,
            purchaseMethod=random.choice(self.purchase_methods),
            saleDate=sale_date,
            storeLocation=random.choice(self.store_locations)
        )
    
    def clear_database(self):
        """Clear all existing data"""
        try:
            Sale.objects.delete()
            logger.info("✅ Cleared existing sales data")
        except Exception as e:
            logger.error(f"❌ Failed to clear database: {e}")
            raise e
    
    def populate_database(self, num_sales: int = 1000):
        """Populate the database with fake sales data"""
        logger.info(f"🔄 Generating {num_sales} sales records...")
        
        try:
            # Generate sales in batches for better performance
            batch_size = 100
            sales_list = []
            
            for i in range(0, num_sales, batch_size):
                batch_count = min(batch_size, num_sales - i)
                batch_sales = [self.generate_sale() for _ in range(batch_count)]
                sales_list.extend(batch_sales)
                
                if (i + batch_size) % 500 == 0:
                    logger.info(f"   Generated {i + batch_size} sales...")
            
            # Insert all sales
            Sale.objects.insert(sales_list)
            logger.info(f"✅ Successfully inserted {len(sales_list)} sales records")
            
            return len(sales_list)
            
        except Exception as e:
            logger.error(f"❌ Failed to populate database: {e}")
            raise e
    
    def generate_seasonal_data(self, year: int = 2024):
        """Generate data with seasonal patterns"""
        logger.info(f"🔄 Generating seasonal data for year {year}...")
        
        seasonal_sales = []
        
        # Generate data for each month with seasonal variations
        for month in range(1, 13):
            # Adjust sales volume based on season
            if month in [11, 12]:  # Holiday season
                monthly_sales = random.randint(150, 200)
            elif month in [6, 7, 8]:  # Summer
                monthly_sales = random.randint(80, 120)
            elif month in [9, 10]:  # Back to school
                monthly_sales = random.randint(100, 150)
            else:  # Regular months
                monthly_sales = random.randint(60, 100)
            
            # Generate sales for this month
            for _ in range(monthly_sales):
                # Random day in the month
                day = random.randint(1, 28)  # Using 28 to avoid date issues
                sale_date = datetime(year, month, day)
                seasonal_sales.append(self.generate_sale(sale_date))
        
        # Insert seasonal data
        Sale.objects.insert(seasonal_sales)
        logger.info(f"✅ Generated {len(seasonal_sales)} seasonal sales records")
        
        return len(seasonal_sales)
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            total_sales = Sale.objects.count()
            
            # Basic statistics
            stats = {
                "total_sales": total_sales,
                "store_locations": len(self.store_locations),
                "purchase_methods": len(self.purchase_methods),
                "product_categories": len(self.product_categories)
            }
            
            if total_sales > 0:
                # Sample some data for analysis
                sample_sales = Sale.objects.limit(100)
                
                # Calculate averages
                total_items = sum(len(sale.items) for sale in sample_sales)
                total_value = sum(
                    sum(float(item.price) * item.quantity for item in sale.items)
                    for sale in sample_sales
                )
                
                stats.update({
                    "avg_items_per_sale": round(total_items / len(sample_sales), 2),
                    "avg_sale_value": round(total_value / len(sample_sales), 2),
                    "coupon_usage_rate": round(
                        Sale.objects.filter(couponUsed=True).count() / total_sales * 100, 2
                    )
                })
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to get database stats: {e}")
            return {"error": str(e)}
    
    def create_indexes(self):
        """Create database indexes for better performance"""
        try:
            # Create indexes on commonly queried fields
            Sale.objects._collection.create_index("saleDate")
            Sale.objects._collection.create_index("storeLocation")
            Sale.objects._collection.create_index("purchaseMethod")
            Sale.objects._collection.create_index("couponUsed")
            Sale.objects._collection.create_index("customer.satisfaction")
            Sale.objects._collection.create_index("customer.age")
            Sale.objects._collection.create_index("customer.gender")
            
            logger.info("✅ Created database indexes")
            
        except Exception as e:
            logger.error(f"❌ Failed to create indexes: {e}")
            raise e


def main():
    """Main function to set up the database"""
    print("🛒 NoSQL Database Setup - Sample Supplies")
    print("=" * 50)
    
    try:
        # Initialize setup
        setup = NoSQLDatabaseSetup()
        
        # Ask user for configuration
        print("\n📋 Configuration Options:")
        print("1. Clear existing data and populate with new data")
        print("2. Add to existing data")
        print("3. Generate seasonal data")
        print("4. Show database statistics")
        print("5. Create indexes only")
        
        choice = input("\nSelect option (1-5): ").strip()
        
        if choice == "1":
            # Clear and populate
            num_sales = int(input("Number of sales to generate (default 1000): ") or "1000")
            setup.clear_database()
            setup.populate_database(num_sales)
            setup.create_indexes()
            
        elif choice == "2":
            # Add to existing
            num_sales = int(input("Number of additional sales to generate: "))
            setup.populate_database(num_sales)
            
        elif choice == "3":
            # Generate seasonal data
            year = int(input("Year for seasonal data (default 2024): ") or "2024")
            setup.generate_seasonal_data(year)
            
        elif choice == "4":
            # Show stats
            stats = setup.get_database_stats()
            print("\n📊 Database Statistics:")
            print(json.dumps(stats, indent=2))
            
        elif choice == "5":
            # Create indexes
            setup.create_indexes()
            
        else:
            print("❌ Invalid choice")
            return
        
        # Show final stats
        if choice in ["1", "2", "3"]:
            print("\n📊 Final Database Statistics:")
            stats = setup.get_database_stats()
            print(json.dumps(stats, indent=2))
        
        print("\n✅ Database setup completed successfully!")
        
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        print("Make sure MONGO_DB is set in your .env file")


if __name__ == "__main__":
    main()
