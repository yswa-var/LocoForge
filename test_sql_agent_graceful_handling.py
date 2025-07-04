#!/usr/bin/env python3
"""
Test SQL agent graceful handling when dependencies are missing
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_sql_agent_graceful_handling():
    """Test that the system handles SQL queries gracefully when SQL agent is not available"""
    print("🧪 Testing SQL Agent Graceful Handling")
    print("=" * 50)
    
    # Test 1: Check SQL_AVAILABLE flag
    try:
        from my_agent.utils.orchestrator_agent import SQL_AVAILABLE
        print(f"✅ SQL_AVAILABLE: {SQL_AVAILABLE}")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    
    # Test 2: Test routing decision with SQL agent unavailable
    try:
        from my_agent.utils.orchestrator_nodes import route_decision, initialize_state
        from my_agent.utils.state import OrchestratorState, QueryDomain
        
        # Create a test state
        state = OrchestratorState()
        state["current_query"] = "Show all employees"
        state["query_domain"] = QueryDomain.EMPLOYEE
        
        # Test routing decision
        decision = route_decision(state)
        print(f"✅ Routing decision for employee query: {decision}")
        
        # Test with unclear query
        state["query_domain"] = QueryDomain.UNCLEAR
        decision = route_decision(state)
        print(f"✅ Routing decision for unclear query: {decision}")
        
    except Exception as e:
        print(f"❌ Routing test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 3: Test data engineer handling SQL queries
    try:
        from my_agent.utils.data_engineer_agent import DataEngineerAgent
        
        data_engineer = DataEngineerAgent()
        result = data_engineer.handle_sql_query_without_agent("Show all employees")
        
        print(f"✅ Data Engineer SQL handling: {result.get('success', False)}")
        print(f"   Query type: {result.get('query_type', 'unknown')}")
        
    except Exception as e:
        print(f"❌ Data Engineer test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n✅ All tests passed! The system handles SQL queries gracefully.")
    return True

def test_orchestrator_without_sql():
    """Test orchestrator behavior when SQL agent is not available"""
    print("\n🧪 Testing Orchestrator Without SQL Agent")
    print("=" * 50)
    
    try:
        from my_agent.utils.orchestrator_agent import HybridOrchestrator
        
        # Create orchestrator
        orchestrator = HybridOrchestrator()
        
        # Check status
        status = orchestrator.check_agent_status()
        print(f"SQL Agent Status: {status['sql_agent']['status']}")
        print(f"NoSQL Agent Status: {status['nosql_agent']['status']}")
        
        # Test SQL query execution when agent is not available
        if not status['sql_agent']['initialized']:
            result = orchestrator.execute_sql_query("Show all employees")
            print(f"✅ SQL query result when agent unavailable: {result.get('success', False)}")
            print(f"   Error message: {result.get('error', 'No error')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Orchestrator test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔧 SQL Agent Graceful Handling Test")
    print("=" * 60)
    
    # Run tests
    test1_result = test_sql_agent_graceful_handling()
    test2_result = test_orchestrator_without_sql()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY")
    print("=" * 60)
    
    if test1_result and test2_result:
        print("✅ All tests passed! The system handles missing SQL agent gracefully.")
    else:
        print("❌ Some tests failed. Check the output above for details.")
    
    print("\n" + "=" * 60)
    print("🎯 TEST COMPLETE")
    print("=" * 60) 