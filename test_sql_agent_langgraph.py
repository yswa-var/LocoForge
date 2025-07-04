#!/usr/bin/env python3
"""
Test script to verify SQL agent works in LangGraph context
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_sql_agent_direct():
    """Test SQL agent directly"""
    print("🧪 Testing SQL Agent Directly")
    print("=" * 50)
    
    try:
        from my_agent.utils.sql_agent import SQLQueryExecutor
        
        # Check environment variables
        openai_key = os.getenv("OPENAPI_KEY") or os.getenv("OPENAI_API_KEY")
        postgres_url = os.getenv("POSTGRES_DB_URL")
        
        print(f"Environment check:")
        print(f"  OPENAI_KEY: {'SET' if openai_key else 'NOT SET'}")
        print(f"  POSTGRES_DB_URL: {'SET' if postgres_url else 'NOT SET'}")
        
        if not openai_key:
            print("❌ OPENAI_KEY not found")
            return False
            
        # Initialize SQL agent
        print("🔄 Initializing SQL agent...")
        sql_agent = SQLQueryExecutor()
        print("✅ SQL agent initialized successfully")
        
        # Test with a simple query
        print("🔄 Testing with simple query...")
        result = sql_agent.generate_and_execute_query("show all employees")
        print(f"✅ Test result: {result.get('success', False)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Direct SQL agent test failed: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

def test_sql_agent_via_orchestrator():
    """Test SQL agent via orchestrator"""
    print("\n🧪 Testing SQL Agent via Orchestrator")
    print("=" * 50)
    
    try:
        from my_agent.utils.orchestrator_agent import HybridOrchestrator
        
        # Initialize orchestrator
        print("🔄 Initializing orchestrator...")
        orchestrator = HybridOrchestrator()
        print("✅ Orchestrator initialized")
        
        # Check agent status
        status = orchestrator.check_agent_status()
        print(f"SQL Agent Status: {status['sql_agent']}")
        
        if not status['sql_agent']['initialized']:
            print("❌ SQL agent not initialized in orchestrator")
            return False
            
        # Test SQL query execution
        print("🔄 Testing SQL query execution...")
        result = orchestrator.execute_sql_query("show all employees")
        print(f"✅ Orchestrator SQL test result: {result.get('success', False)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Orchestrator SQL agent test failed: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

def test_sql_agent_via_langgraph():
    """Test SQL agent via LangGraph workflow"""
    print("\n🧪 Testing SQL Agent via LangGraph Workflow")
    print("=" * 50)
    
    try:
        from my_agent.agent import graph
        from my_agent.utils.state import OrchestratorState
        from langchain_core.messages import HumanMessage
        
        # Create test state
        state = OrchestratorState(
            messages=[HumanMessage(content="show all employees")],
            current_query="show all employees"
        )
        
        print("🔄 Running LangGraph workflow...")
        result = graph.invoke(state)
        print("✅ LangGraph workflow completed")
        
        # Check if SQL agent was used
        execution_path = result.get("execution_path", [])
        print(f"Execution path: {execution_path}")
        
        if "sql_agent" in execution_path:
            print("✅ SQL agent was used in workflow")
            
            # Check results
            sql_results = result.get("sql_results", {})
            if sql_results and sql_results.get("success", False):
                print("✅ SQL agent returned successful results")
                return True
            else:
                print(f"❌ SQL agent failed: {sql_results.get('error', 'Unknown error')}")
                return False
        else:
            print("❌ SQL agent was not used in workflow")
            return False
            
    except Exception as e:
        print(f"❌ LangGraph SQL agent test failed: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

def main():
    """Run all tests"""
    print("🚀 SQL Agent LangGraph Integration Test")
    print("=" * 60)
    
    # Test 1: Direct SQL agent
    test1_passed = test_sql_agent_direct()
    
    # Test 2: SQL agent via orchestrator
    test2_passed = test_sql_agent_via_orchestrator()
    
    # Test 3: SQL agent via LangGraph
    test3_passed = test_sql_agent_via_langgraph()
    
    # Summary
    print("\n📊 Test Results Summary")
    print("=" * 30)
    print(f"Direct SQL Agent: {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"Orchestrator SQL Agent: {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    print(f"LangGraph SQL Agent: {'✅ PASSED' if test3_passed else '❌ FAILED'}")
    
    if all([test1_passed, test2_passed, test3_passed]):
        print("\n🎉 All tests passed! SQL agent is working correctly in LangGraph.")
    else:
        print("\n⚠️ Some tests failed. Check the error messages above.")
        
        if not test1_passed:
            print("💡 Direct SQL agent test failed - check environment variables and database connection")
        if not test2_passed:
            print("💡 Orchestrator SQL agent test failed - check orchestrator initialization")
        if not test3_passed:
            print("💡 LangGraph SQL agent test failed - check workflow configuration")

if __name__ == "__main__":
    main() 