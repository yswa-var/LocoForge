#!/usr/bin/env python3
"""
Test script to check SQL agent initialization and provide debugging information
"""

import os
import sys
from dotenv import load_dotenv

# Add the my_agent directory to the path
sys.path.append('my_agent')

def test_environment():
    """Test environment variables"""
    print("🔍 Environment Variables Check:")
    print(f"  OPENAPI_KEY: {'SET' if os.getenv('OPENAPI_KEY') else 'NOT SET'}")
    print(f"  OPENAI_API_KEY: {'SET' if os.getenv('OPENAI_API_KEY') else 'NOT SET'}")
    print(f"  POSTGRES_DB_URL: {'SET' if os.getenv('POSTGRES_DB_URL') else 'NOT SET'}")
    print(f"  SQL_DB: {'SET' if os.getenv('SQL_DB') else 'NOT SET'}")
    
    # Check if any OpenAI key is available
    openai_key = os.getenv('OPENAPI_KEY') or os.getenv('OPENAI_API_KEY')
    if not openai_key:
        print("\n❌ No OpenAI API key found!")
        print("   Please set either OPENAPI_KEY or OPENAI_API_KEY environment variable")
        return False
    else:
        print(f"\n✅ OpenAI API key found: {openai_key[:10]}...")
        return True

def test_sql_agent_import():
    """Test if SQL agent can be imported"""
    print("\n🔍 SQL Agent Import Test:")
    try:
        from my_agent.utils.sql_agent import SQLQueryExecutor
        print("✅ SQL agent import successful")
        return True
    except ImportError as e:
        print(f"❌ SQL agent import failed: {e}")
        return False

def test_sql_agent_initialization():
    """Test SQL agent initialization"""
    print("\n🔍 SQL Agent Initialization Test:")
    try:
        from my_agent.utils.sql_agent import SQLQueryExecutor
        
        # Try to initialize the agent
        sql_agent = SQLQueryExecutor()
        print("✅ SQL agent initialization successful")
        return True
    except Exception as e:
        print(f"❌ SQL agent initialization failed: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False

def test_orchestrator():
    """Test orchestrator initialization"""
    print("\n🔍 Orchestrator Test:")
    try:
        from my_agent.utils.orchestrator_agent import HybridOrchestrator
        
        orchestrator = HybridOrchestrator()
        status = orchestrator.check_agent_status()
        
        print("✅ Orchestrator initialization successful")
        print(f"   SQL Agent: {status['sql_agent']['status']}")
        print(f"   NoSQL Agent: {status['nosql_agent']['status']}")
        
        return status['sql_agent']['initialized']
    except Exception as e:
        print(f"❌ Orchestrator initialization failed: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False

def test_sql_query():
    """Test a simple SQL query"""
    print("\n🔍 SQL Query Test:")
    try:
        from my_agent.utils.orchestrator_agent import HybridOrchestrator
        
        orchestrator = HybridOrchestrator()
        result = orchestrator.execute_sql_query("Show all employees in the company")
        
        if result.get('success', False):
            print("✅ SQL query execution successful")
            print(f"   Generated SQL: {result.get('generated_sql', 'N/A')}")
            print(f"   Row count: {result.get('execution_result', {}).get('row_count', 0)}")
        else:
            print(f"❌ SQL query execution failed: {result.get('error', 'Unknown error')}")
        
        return result.get('success', False)
    except Exception as e:
        print(f"❌ SQL query test failed: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False

def main():
    """Run all tests"""
    print("🚀 SQL Agent Fix Test Suite")
    print("=" * 50)
    
    # Load environment variables
    load_dotenv()
    
    # Run tests
    env_ok = test_environment()
    import_ok = test_sql_agent_import()
    init_ok = test_sql_agent_initialization()
    orchestrator_ok = test_orchestrator()
    query_ok = test_sql_query()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Summary:")
    print(f"  Environment: {'✅ PASS' if env_ok else '❌ FAIL'}")
    print(f"  Import: {'✅ PASS' if import_ok else '❌ FAIL'}")
    print(f"  Initialization: {'✅ PASS' if init_ok else '❌ FAIL'}")
    print(f"  Orchestrator: {'✅ PASS' if orchestrator_ok else '❌ FAIL'}")
    print(f"  Query Execution: {'✅ PASS' if query_ok else '❌ FAIL'}")
    
    if query_ok:
        print("\n🎉 All tests passed! SQL agent is working correctly.")
    else:
        print("\n🔧 Some tests failed. Check the error messages above for debugging.")

if __name__ == "__main__":
    main() 