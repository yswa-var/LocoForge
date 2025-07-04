#!/usr/bin/env python3
"""
Test script for NoSQL Query Executor
Tests the NoSQL agent with sample queries
"""

import os
import json
from dotenv import load_dotenv
from my_agent.utils.nosql_agent import create_nosql_agent

# Load environment variables
load_dotenv()

def test_nosql_agent():
    """Test the NoSQL agent with various queries"""
    print("🧪 Testing NoSQL Query Executor")
    print("=" * 50)
    
    try:
        # Create agent
        agent = create_nosql_agent()
        print("✅ NoSQL agent created successfully")
        
        # Test queries
        test_queries = [
            "Show all products",
            "Find products with low stock",
            "Show high-value orders above $500",
            "List products by category"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n🔍 Test {i}: {query}")
            print("-" * 30)
            
            try:
                result = agent.generate_and_execute_query(query)
                
                # Display structured result
                print("📊 Result:")
                print(json.dumps(result, indent=2, default=str))
                
                if result["execution_result"]["success"]:
                    print(f"✅ Query executed successfully - {result['execution_result']['row_count']} results")
                else:
                    print(f"❌ Query failed: {result['execution_result']['error']}")
                    
            except Exception as e:
                print(f"❌ Error executing query: {e}")
        
        print("\n" + "=" * 50)
        print("🎉 NoSQL Agent Testing Complete!")
        
    except Exception as e:
        print(f"❌ Failed to create NoSQL agent: {e}")
        print("Make sure MONGO_DB and OPENAPI_KEY are set in your .env file")
    finally:
        if 'agent' in locals():
            agent.close_connection()

if __name__ == "__main__":
    # Check environment variables
    if not os.getenv("OPENAPI_KEY"):
        print("❌ Error: OPENAPI_KEY not found in environment variables")
        exit(1)
    
    if not os.getenv("MONGO_DB"):
        print("❌ Error: MONGO_DB not found in environment variables")
        exit(1)
    
    test_nosql_agent() 