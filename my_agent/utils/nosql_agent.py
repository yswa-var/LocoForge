#!/usr/bin/env python3
"""
NoSQL Query Executor using OpenAI
Executes MongoDB queries against the grocery warehouse database
Returns structured JSON output with query and results
"""

import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage

# Load environment variables
load_dotenv()

class NoSQLQueryExecutor:
    """NoSQL Query Executor for Grocery Warehouse Database"""
    
    def __init__(self, connection_string: str = None):
        """
        Initialize the NoSQL Query Executor
        
        Args:
            connection_string: MongoDB connection string (defaults to MONGO_DB from .env)
        """
        self.connection_string = connection_string or "mongodb://localhost:27017/"
        if not self.connection_string:
            raise ValueError("MongoDB connection string not found. Set MONGO_DB in .env file")
        
        self.client = MongoClient(self.connection_string)
        self.db_name = "grocery_warehouse"
        self.db = self.client[self.db_name]
        
        self.model = ChatOpenAI(
            model="gpt-4o-mini",
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        
        # Database schema context
        self.db_context = self._build_database_context()
        
    def _build_database_context(self) -> str:
        """Build comprehensive database context including schema and relationships"""
        context = """
MONGODB DATABASE SCHEMA FOR GROCERY WAREHOUSE SYSTEM:

DATABASE: grocery_warehouse

1. PRODUCTS COLLECTION:
   Document Structure:
   {
     "_id": ObjectId,
     "product_id": "PROD001" (String, unique),
     "name": "Organic Bananas" (String),
     "category": "Fruits" (String),
     "subcategory": "Tropical Fruits" (String),
     "brand": "FreshHarvest" (String),
     "description": "Premium organic bananas..." (String),
     "specifications": {
       "weight_per_unit": "150g" (String),
       "origin": "Ecuador" (String),
       "organic_certified": true (Boolean),
       "allergens": [] (Array of Strings),
       "nutritional_info": {
         "calories_per_100g": 89 (Number),
         "protein": "1.1g" (String),
         "carbs": "23g" (String),
         "fiber": "2.6g" (String)
       }
     },
     "pricing": {
       "cost_price": 0.45 (Number),
       "selling_price": 0.89 (Number),
       "currency": "USD" (String),
       "bulk_discounts": [
         {"quantity": 10, "discount_percent": 5} (Object)
       ]
     },
     "supplier_info": {
       "supplier_id": "SUPP001" (String),
       "supplier_name": "Tropical Fruits Co." (String),
       "contact": {
         "email": "orders@tropicalfruits.com" (String),
         "phone": "+1-555-0123" (String)
       },
       "lead_time_days": 3 (Number)
     },
     "created_at": ISODate,
     "updated_at": ISODate,
     "active": true (Boolean)
   }

2. INVENTORY COLLECTION:
   Document Structure:
   {
     "_id": ObjectId,
     "inventory_id": "INV001" (String, unique),
     "product_id": "PROD001" (String, references products.product_id),
     "warehouse_location": {
       "zone": "A" (String),
       "aisle": "1" (String),
       "shelf": "3" (String),
       "position": "left" (String)
     },
     "stock_levels": {
       "current_stock": 1250 (Number),
       "minimum_stock": 200 (Number),
       "maximum_stock": 2000 (Number),
       "reorder_point": 300 (Number)
     },
     "batch_info": [
       {
         "batch_id": "BATCH001-001" (String),
         "quantity": 800 (Number),
         "expiry_date": ISODate,
         "supplier_batch": "TB-2024-001" (String),
         "received_date": ISODate,
         "quality_status": "excellent" (String)
       }
     ],
     "movement_history": [
       {
         "date": ISODate,
         "type": "in" (String: "in" or "out"),
         "quantity": 450 (Number),
         "reference": "PO-2024-001" (String)
       }
     ],
     "last_updated": ISODate
   }

3. ORDERS COLLECTION:
   Document Structure:
   {
     "_id": ObjectId,
     "order_id": "ORD001" (String, unique),
     "customer_info": {
       "customer_id": "CUST001" (String),
       "name": "Fresh Market Chain" (String),
       "type": "retail_chain" (String),
       "contact": {
         "email": "orders@freshmarket.com" (String),
         "phone": "+1-555-1000" (String),
         "address": {
           "street": "123 Market St" (String),
           "city": "Downtown" (String),
           "state": "CA" (String),
           "zip": "90210" (String)
         }
       },
       "credit_limit": 50000 (Number),
       "payment_terms": "net_30" (String)
     },
     "order_details": {
       "order_date": ISODate,
       "delivery_date": ISODate,
       "status": "processing" (String),
       "priority": "high" (String),
       "delivery_method": "express" (String),
       "special_instructions": "Keep refrigerated..." (String)
     },
     "items": [
       {
         "product_id": "PROD001" (String, references products.product_id),
         "quantity": 500 (Number),
         "unit_price": 0.89 (Number),
         "total_price": 445.00 (Number),
         "discount_applied": 0.05 (Number),
         "final_price": 422.75 (Number)
       }
     ],
     "pricing": {
       "subtotal": 803.00 (Number),
       "tax_rate": 0.085 (Number),
       "tax_amount": 68.26 (Number),
       "shipping_cost": 25.00 (Number),
       "total_amount": 896.26 (Number)
     },
     "payment_info": {
       "method": "credit_card" (String),
       "status": "pending" (String),
       "transaction_id": null (String or null)
     },
     "created_at": ISODate,
     "updated_at": ISODate
   }

RELATIONSHIPS:
- Products -> Inventory: One-to-One (product_id)
- Products -> Orders: One-to-Many (via items.product_id)
- Inventory tracks stock for Products
- Orders contain multiple Products via items array

COMMON QUERY PATTERNS:
- $lookup to join collections (products with inventory, orders with products)
- $match for filtering (stock levels, order status, product category)
- $group for aggregations (total sales, average prices, stock counts)
- $unwind for array operations (batch_info, items, movement_history)
- $project for field selection and calculations
- $sort for ordering results
- $limit for result limiting
- Date range queries with $gte, $lte
- Array element matching with $elemMatch
- Nested object queries using dot notation
"""
        return context
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a MongoDB query and return results
        
        Args:
            query: MongoDB query (JSON string or aggregation pipeline)
            
        Returns:
            Dictionary with query results and metadata
        """
        try:
            # Parse the query
            if query.strip().startswith('['):
                # Aggregation pipeline
                pipeline = json.loads(query)
                results = list(self.db.products.aggregate(pipeline))
                # Try other collections if products doesn't work
                if not results:
                    results = list(self.db.inventory.aggregate(pipeline))
                if not results:
                    results = list(self.db.orders.aggregate(pipeline))
            else:
                # Find query
                query_dict = json.loads(query)
                collection_name = query_dict.get('collection', 'products')
                find_query = query_dict.get('query', {})
                projection = query_dict.get('projection', {})
                
                collection = getattr(self.db, collection_name)
                results = list(collection.find(find_query, projection))
            
            # Convert ObjectId to string for JSON serialization
            def convert_objectid(obj):
                if isinstance(obj, dict):
                    return {k: convert_objectid(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_objectid(item) for item in obj]
                elif hasattr(obj, '__class__') and obj.__class__.__name__ == 'ObjectId':
                    return str(obj)
                elif isinstance(obj, datetime):
                    return obj.isoformat()
                else:
                    return obj
            
            converted_results = convert_objectid(results)
            
            return {
                "success": True,
                "query": query,
                "row_count": len(converted_results),
                "data": converted_results
            }
            
        except Exception as e:
            return {
                "success": False,
                "query": query,
                "error": str(e),
                "data": []
            }
    
    def generate_and_execute_query(self, prompt: str) -> Dict[str, Any]:
        """
        Generate MongoDB query from natural language prompt and execute it
        
        Args:
            prompt: Natural language description of what data to retrieve
            
        Returns:
            Structured response with generated query and results
        """
        # Create system prompt with database context
        system_prompt = """
You are a MongoDB query generator for a Grocery Warehouse Management System. 

""" + self.db_context + """

INSTRUCTIONS:
1. Generate ONLY valid MongoDB queries (find operations) or aggregation pipelines
2. Use appropriate $lookup for joining collections
3. Use $match for filtering conditions
4. Use $project for field selection
5. Use $sort for meaningful ordering
6. Use $limit to limit results (default 50 if not specified)
7. Return ONLY the MongoDB query in JSON format, no explanations

FOR FIND QUERIES, return format:
{
  "collection": "collection_name",
  "query": { "field": "value" },
  "projection": { "field": 1, "_id": 0 }
}

FOR AGGREGATION PIPELINES, return format:
[
  { "$match": { "field": "value" } },
  { "$lookup": { "from": "collection", "localField": "field", "foreignField": "field", "as": "alias" } },
  { "$project": { "field": 1 } }
]

EXAMPLE PROMPTS AND QUERIES:
- "Show all products": { "collection": "products", "query": {}, "projection": { "_id": 0 } }
- "Products with low stock": [{ "$lookup": { "from": "inventory", "localField": "product_id", "foreignField": "product_id", "as": "inventory" } }, { "$unwind": "$inventory" }, { "$match": { "$expr": { "$lte": ["$inventory.stock_levels.current_stock", "$inventory.stock_levels.reorder_point"] } } }, { "$project": { "product_id": 1, "name": 1, "current_stock": "$inventory.stock_levels.current_stock" } }]
- "High value orders": { "collection": "orders", "query": { "pricing.total_amount": { "$gte": 500 } }, "projection": { "_id": 0, "order_id": 1, "pricing.total_amount": 1, "customer_info.name": 1 } }
"""
        
        # Generate MongoDB query
        messages = [
            HumanMessage(content=f"System: {system_prompt}"),
            HumanMessage(content=f"Generate MongoDB query for: {prompt}")
        ]
        
        response = self.model.invoke(messages)
        generated_query = response.content.strip()
        
        # Clean up the query (remove markdown if present)
        if generated_query.startswith("```json"):
            generated_query = generated_query.replace("```json", "").replace("```", "").strip()
        elif generated_query.startswith("```"):
            generated_query = generated_query.replace("```", "").strip()
        
        # Execute the generated query
        query_result = self.execute_query(generated_query)
        
        # Return structured response
        return {
            "prompt": prompt,
            "generated_mongodb_query": generated_query,
            "execution_result": query_result,
            "timestamp": self._get_timestamp()
        }
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        return datetime.now().isoformat()
    
    def get_sample_queries(self) -> List[str]:
        """Get sample query prompts for testing"""
        return [
            "Show all products with their current stock levels",
            "Find products that need reordering (low stock)",
            "Show high-value orders above $500",
            "List products expiring within 7 days",
            "Find customers who made multiple orders",
            "Show inventory movement history for the last 24 hours",
            "Calculate total sales by product category",
            "Find products with quality issues",
            "Show warehouse zone utilization",
            "List suppliers and their product counts"
        ]
    
    def close_connection(self):
        """Close the MongoDB connection"""
        if self.client:
            self.client.close()

def create_nosql_agent() -> NoSQLQueryExecutor:
    """Create a pre-configured NoSQL Query Executor"""
    return NoSQLQueryExecutor()

def interactive_nosql_chat():
    """Run an interactive NoSQL query session"""
    print("🗄️  NoSQL Query Executor (MongoDB)")
    print("=" * 50)
    
    try:
        agent = create_nosql_agent()
        print("✅ Connected to MongoDB successfully")
        print("📊 Database context loaded")
        
        print("\n💡 Sample queries you can try:")
        sample_queries = agent.get_sample_queries()
        for i, query in enumerate(sample_queries[:5], 1):
            print(f"{i}. {query}")
        
        print("\nType 'quit' to exit, 'samples' to see more examples")
        print("-" * 50)
        
        while True:
            try:
                user_input = input("\n🔍 Enter your query: ").strip()
                
                if user_input.lower() == 'quit':
                    print("👋 Goodbye!")
                    break
                elif user_input.lower() == 'samples':
                    print("\n📋 Sample Queries:")
                    for i, query in enumerate(sample_queries, 1):
                        print(f"{i}. {query}")
                    continue
                elif not user_input:
                    continue
                
                # Generate and execute query
                print("🤖 Generating MongoDB query...")
                result = agent.generate_and_execute_query(user_input)
                
                # Display results in structured format
                print("\n📊 RESULTS:")
                print(json.dumps(result, indent=2, default=str))
                
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}")
                
    except Exception as e:
        print(f"❌ Failed to initialize NoSQL agent: {e}")
        print("Make sure MONGO_DB and OPENAI_API_KEY are set in your .env file")
    finally:
        if 'agent' in locals():
            agent.close_connection()

if __name__ == "__main__":
    # Check if required environment variables are available
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY not found in environment variables")
        print("Please make sure your .env file contains: OPENAI_API_KEY=your_api_key_here")
        exit(1)
    
    if not os.getenv("MONGO_DB"):
        print("❌ Error: MONGO_DB not found in environment variables")
        print("Please make sure your .env file contains: MONGO_DB=mongodb://localhost:27017/")
        exit(1)
    
    interactive_nosql_chat()
