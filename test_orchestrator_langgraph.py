#!/usr/bin/env python3
"""
Test script to verify orchestrator works in LangGraph context
"""

import os
import json
from dotenv import load_dotenv
from my_agent.utils.orchestrator_nodes import get_orchestrator, reset_orchestrator

# Load environment variables
load_dotenv()

def test_orchestrator_in_langgraph_context():
    """Test the orchestrator in a LangGraph-like context"""
    print("🧪 Testing Orchestrator in LangGraph Context")
    print("=" * 50)
    
    try:
        # Reset orchestrator to force re-initialization
        print("🔄 Resetting orchestrator...")
        orchestrator = reset_orchestrator()
        
        # Check status
        print("\n📊 Checking agent status...")
        status = orchestrator.check_agent_status()
        print(json.dumps(status, indent=2))
        
        # Test NoSQL query directly
        print("\n🔍 Testing NoSQL query...")
        test_query = "Show all products"
        result = orchestrator.execute_nosql_query(test_query)
        
        print(f"Query: {test_query}")
        print(f"Success: {result.get('execution_result', {}).get('success', False)}")
        
        if result.get('execution_result', {}).get('success', False):
            print(f"✅ NoSQL query executed successfully - {result.get('execution_result', {}).get('row_count', 0)} results")
        else:
            print(f"❌ NoSQL query failed: {result.get('execution_result', {}).get('error', 'Unknown error')}")
        
        # Test SQL query directly
        print("\n🔍 Testing SQL query...")
        test_query = "Show all employees"
        result = orchestrator.execute_sql_query(test_query)
        
        print(f"Query: {test_query}")
        print(f"Success: {result.get('execution_result', {}).get('success', False)}")
        
        if result.get('execution_result', {}).get('success', False):
            print(f"✅ SQL query executed successfully - {result.get('execution_result', {}).get('row_count', 0)} results")
        else:
            print(f"❌ SQL query failed: {result.get('execution_result', {}).get('error', 'Unknown error')}")
        
        print("\n" + "=" * 50)
        print("🎉 Orchestrator LangGraph Context Test Complete!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Check environment variables
    print("🔍 Environment Check:")
    print(f"MONGO_DB: {'SET' if os.getenv('MONGO_DB') else 'NOT SET'}")
    print(f"OPENAI_API_KEY: {'SET' if os.getenv('OPENAI_API_KEY') else 'NOT SET'}")
    print(f"SQL_DB: {'SET' if os.getenv('SQL_DB') else 'NOT SET'}")
    print()
    
    test_orchestrator_in_langgraph_context() 