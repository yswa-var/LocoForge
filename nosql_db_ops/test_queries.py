import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import json
from nosql_db_init import GroceryWarehouseDB

class GroceryWarehouseQueries:
    def __init__(self):
        """Initialize connection to the grocery warehouse database."""
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["grocery_warehouse"]
    
    def test_complex_queries(self):
        """Test various complex queries to demonstrate NoSQL capabilities."""
        print("="*60)
        print("GROCERY WAREHOUSE - COMPLEX NOSQL QUERIES")
        print("="*60)
        
        # Query 1: Find products with low stock that need reordering
        print("\n1. PRODUCTS NEEDING REORDER (Complex Aggregation):")
        print("-" * 40)
        pipeline = [
            {
                "$lookup": {
                    "from": "inventory",
                    "localField": "product_id",
                    "foreignField": "product_id",
                    "as": "inventory_info"
                }
            },
            {
                "$unwind": "$inventory_info"
            },
            {
                "$match": {
                    "$expr": {
                        "$lte": [
                            "$inventory_info.stock_levels.current_stock",
                            "$inventory_info.stock_levels.reorder_point"
                        ]
                    }
                }
            },
            {
                "$project": {
                    "product_id": 1,
                    "name": 1,
                    "category": 1,
                    "current_stock": "$inventory_info.stock_levels.current_stock",
                    "reorder_point": "$inventory_info.stock_levels.reorder_point",
                    "supplier_name": "$supplier_info.supplier_name",
                    "lead_time": "$supplier_info.lead_time_days"
                }
            }
        ]
        
        low_stock_products = list(self.db.products.aggregate(pipeline))
        for product in low_stock_products:
            print(f"  {product['name']} (ID: {product['product_id']})")
            print(f"    Stock: {product['current_stock']} | Reorder Point: {product['reorder_point']}")
            print(f"    Supplier: {product['supplier_name']} | Lead Time: {product['lead_time']} days")
            print()
        
        # Query 2: Find products expiring soon
        print("\n2. PRODUCTS EXPIRING SOON (Nested Array Query):")
        print("-" * 40)
        expiry_threshold = datetime.now() + timedelta(days=7)
        
        expiring_products = self.db.inventory.find({
            "batch_info": {
                "$elemMatch": {
                    "expiry_date": {"$lte": expiry_threshold}
                }
            }
        })
        
        for inv in expiring_products:
            product = self.db.products.find_one({"product_id": inv["product_id"]})
            expiring_batches = [batch for batch in inv["batch_info"] 
                              if batch["expiry_date"] <= expiry_threshold]
            
            print(f"  {product['name']} (ID: {inv['product_id']})")
            for batch in expiring_batches:
                days_left = (batch["expiry_date"] - datetime.now()).days
                print(f"    Batch {batch['batch_id']}: {batch['quantity']} units, expires in {days_left} days")
            print()
        
        # Query 3: High-value orders with complex customer analysis
        print("\n3. HIGH-VALUE ORDERS ANALYSIS (Complex Projection):")
        print("-" * 40)
        high_value_orders = self.db.orders.find({
            "pricing.total_amount": {"$gte": 500}
        }).sort("pricing.total_amount", -1)
        
        for order in high_value_orders:
            print(f"  Order {order['order_id']}: ${order['pricing']['total_amount']:.2f}")
            print(f"    Customer: {order['customer_info']['name']} ({order['customer_info']['type']})")
            print(f"    Status: {order['order_details']['status']}")
            print(f"    Items: {len(order['items'])} products")
            print(f"    Payment: {order['payment_info']['method']} - {order['payment_info']['status']}")
            print()
        
        # Query 4: Warehouse zone utilization
        print("\n4. WAREHOUSE ZONE UTILIZATION (Grouping & Aggregation):")
        print("-" * 40)
        zone_pipeline = [
            {
                "$group": {
                    "_id": "$warehouse_location.zone",
                    "total_products": {"$sum": 1},
                    "total_stock": {"$sum": "$stock_levels.current_stock"},
                    "avg_stock": {"$avg": "$stock_levels.current_stock"},
                    "products": {"$push": "$product_id"}
                }
            },
            {
                "$sort": {"total_stock": -1}
            }
        ]
        
        zone_stats = list(self.db.inventory.aggregate(zone_pipeline))
        for zone in zone_stats:
            print(f"  Zone {zone['_id']}:")
            print(f"    Products: {zone['total_products']}")
            print(f"    Total Stock: {zone['total_stock']} units")
            print(f"    Average Stock: {zone['avg_stock']:.1f} units")
            print()
        
        # Query 5: Product category performance
        print("\n5. PRODUCT CATEGORY PERFORMANCE (Multi-collection Join):")
        print("-" * 40)
        category_pipeline = [
            {
                "$lookup": {
                    "from": "inventory",
                    "localField": "product_id",
                    "foreignField": "product_id",
                    "as": "inventory_data"
                }
            },
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "product_id",
                    "foreignField": "items.product_id",
                    "as": "order_data"
                }
            },
            {
                "$group": {
                    "_id": "$category",
                    "product_count": {"$sum": 1},
                    "avg_price": {"$avg": "$pricing.selling_price"},
                    "total_inventory": {
                        "$sum": {
                            "$reduce": {
                                "input": "$inventory_data",
                                "initialValue": 0,
                                "in": {"$add": ["$$value", "$$this.stock_levels.current_stock"]}
                            }
                        }
                    },
                    "order_count": {"$sum": {"$size": "$order_data"}}
                }
            },
            {
                "$sort": {"order_count": -1}
            }
        ]
        
        category_performance = list(self.db.products.aggregate(category_pipeline))
        for category in category_performance:
            print(f"  {category['_id']}:")
            print(f"    Products: {category['product_count']}")
            print(f"    Avg Price: ${category['avg_price']:.2f}")
            print(f"    Total Inventory: {category['total_inventory']} units")
            print(f"    Orders: {category['order_count']}")
            print()
        
        # Query 6: Customer order patterns
        print("\n6. CUSTOMER ORDER PATTERNS (Complex Array Analysis):")
        print("-" * 40)
        customer_pipeline = [
            {
                "$group": {
                    "_id": "$customer_info.customer_id",
                    "customer_name": {"$first": "$customer_info.name"},
                    "customer_type": {"$first": "$customer_info.type"},
                    "total_orders": {"$sum": 1},
                    "total_spent": {"$sum": "$pricing.total_amount"},
                    "avg_order_value": {"$avg": "$pricing.total_amount"},
                    "payment_methods": {"$addToSet": "$payment_info.method"},
                    "order_statuses": {"$addToSet": "$order_details.status"}
                }
            },
            {
                "$sort": {"total_spent": -1}
            }
        ]
        
        customer_patterns = list(self.db.orders.aggregate(customer_pipeline))
        for customer in customer_patterns:
            print(f"  {customer['customer_name']} ({customer['customer_type']}):")
            print(f"    Orders: {customer['total_orders']}")
            print(f"    Total Spent: ${customer['total_spent']:.2f}")
            print(f"    Avg Order: ${customer['avg_order_value']:.2f}")
            print(f"    Payment Methods: {', '.join(customer['payment_methods'])}")
            print(f"    Order Statuses: {', '.join(customer['order_statuses'])}")
            print()
    
    def test_advanced_queries(self):
        """Test more advanced queries with complex conditions."""
        print("\n" + "="*60)
        print("ADVANCED NOSQL QUERIES")
        print("="*60)
        
        # Query 7: Products with multiple batches and quality issues
        print("\n7. PRODUCTS WITH QUALITY ISSUES (Array Filtering):")
        print("-" * 40)
        quality_issues = self.db.inventory.find({
            "batch_info": {
                "$elemMatch": {
                    "quality_status": {"$ne": "excellent"}
                }
            }
        })
        
        for inv in quality_issues:
            product = self.db.products.find_one({"product_id": inv["product_id"]})
            problematic_batches = [batch for batch in inv["batch_info"] 
                                 if batch["quality_status"] != "excellent"]
            
            print(f"  {product['name']} (ID: {inv['product_id']})")
            for batch in problematic_batches:
                print(f"    Batch {batch['batch_id']}: {batch['quality_status']} quality")
            print()
        
        # Query 8: Recent inventory movements
        print("\n8. RECENT INVENTORY MOVEMENTS (Date Range & Array Analysis):")
        print("-" * 40)
        recent_movements = self.db.inventory.find({
            "movement_history.date": {
                "$gte": datetime.now() - timedelta(days=1)
            }
        })
        
        for inv in recent_movements:
            product = self.db.products.find_one({"product_id": inv["product_id"]})
            recent_moves = [move for move in inv["movement_history"] 
                          if move["date"] >= datetime.now() - timedelta(days=1)]
            
            print(f"  {product['name']} (ID: {inv['product_id']})")
            for move in recent_moves:
                print(f"    {move['type'].upper()}: {move['quantity']} units ({move['reference']})")
            print()
        
        # Query 9: Supplier performance analysis
        print("\n9. SUPPLIER PERFORMANCE (Nested Object Analysis):")
        print("-" * 40)
        supplier_pipeline = [
            {
                "$group": {
                    "_id": "$supplier_info.supplier_id",
                    "supplier_name": {"$first": "$supplier_info.supplier_name"},
                    "product_count": {"$sum": 1},
                    "avg_lead_time": {"$avg": "$supplier_info.lead_time_days"},
                    "categories": {"$addToSet": "$category"},
                    "organic_products": {
                        "$sum": {
                            "$cond": [
                                "$specifications.organic_certified",
                                1,
                                0
                            ]
                        }
                    }
                }
            },
            {
                "$sort": {"product_count": -1}
            }
        ]
        
        supplier_performance = list(self.db.products.aggregate(supplier_pipeline))
        for supplier in supplier_performance:
            print(f"  {supplier['supplier_name']}:")
            print(f"    Products: {supplier['product_count']}")
            print(f"    Avg Lead Time: {supplier['avg_lead_time']:.1f} days")
            print(f"    Categories: {', '.join(supplier['categories'])}")
            print(f"    Organic Products: {supplier['organic_products']}")
            print()
    
    def close_connection(self):
        """Close the MongoDB connection."""
        self.client.close()

def main():
    """Main function to run the query tests."""
    try:
        # First ensure database exists
        warehouse_db = GroceryWarehouseDB()
        if not warehouse_db.check_database_exists():
            print("Database doesn't exist. Creating it first...")
            warehouse_db.create_database()
        warehouse_db.close_connection()
        
        # Run query tests
        query_tester = GroceryWarehouseQueries()
        query_tester.test_complex_queries()
        query_tester.test_advanced_queries()
        query_tester.close_connection()
        
        print("\n" + "="*60)
        print("ALL QUERIES COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\nThis demonstrates the complex NoSQL capabilities:")
        print("- Multi-collection joins and aggregations")
        print("- Nested object and array queries")
        print("- Complex filtering and grouping")
        print("- Date range operations")
        print("- Array element matching")
        print("- Performance optimization with indexes")
        
    except Exception as e:
        print(f"Error running queries: {e}")
        print("Please ensure MongoDB is running on localhost:27017")

if __name__ == "__main__":
    main()
