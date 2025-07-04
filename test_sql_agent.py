#!/usr/bin/env python3
"""
Test script for SQL Query Executor
Demonstrates the functionality with sample queries
"""

import json
from my_agent.utils.sql_agent import SQLQueryExecutor

def test_sql_agent():
    """Test the SQL agent with various queries"""
    print("🧪 Testing SQL Query Executor")
    print("=" * 50)
    
    try:
        # Create SQL agent
        agent = SQLQueryExecutor()
        print("✅ SQL Agent initialized successfully")
        
        # Test queries
        test_queries = [
            "Show me all employees",
            "Calculate average salary by department",
            "Find employees who are managers",
            "Show projects and their assigned employees",
            "List departments with their total budget"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n🔍 Test {i}: {query}")
            print("-" * 40)
            
            try:
                result = agent.generate_and_execute_query(query)
                
                # Display results in a clean format
                print(f"📝 Prompt: {result['prompt']}")
                print(f"🔧 Generated SQL: {result['generated_sql']}")
                
                if result['execution_result']['success']:
                    print(f"✅ Success: {result['execution_result']['row_count']} rows returned")
                    if result['execution_result']['data']:
                        print("📊 Sample data (first 3 rows):")
                        for j, row in enumerate(result['execution_result']['data'][:3], 1):
                            print(f"  Row {j}: {row}")
                        if len(result['execution_result']['data']) > 3:
                            print(f"  ... and {len(result['execution_result']['data']) - 3} more rows")
                else:
                    print(f"❌ Error: {result['execution_result']['error']}")
                
                print(f"⏰ Timestamp: {result['timestamp']}")
                
            except Exception as e:
                print(f"❌ Test failed: {e}")
            
            print()
        
        # Show full JSON structure for one query
        print("\n📋 Full JSON Structure Example:")
        print("=" * 50)
        sample_result = agent.generate_and_execute_query("Show all departments")
        print(json.dumps(sample_result, indent=2, default=str))
        
    except Exception as e:
        print(f"❌ Failed to initialize SQL agent: {e}")
        print("Make sure SQL_DB and OPENAPI_KEY are set in your .env file")

if __name__ == "__main__":
    test_sql_agent() 