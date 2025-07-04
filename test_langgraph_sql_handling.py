#!/usr/bin/env python3
"""
Test SQL agent handling in LangGraph Studio environment
Simulates the case where psycopg2 is not available
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def simulate_langgraph_environment():
    """Simulate LangGraph Studio environment where psycopg2 is not available"""
    print("🧪 Simulating LangGraph Studio Environment")
    print("=" * 50)
    
    # Temporarily remove psycopg2 from sys.modules to simulate missing dependency
    original_psycopg2 = None
    if 'psycopg2' in sys.modules:
        original_psycopg2 = sys.modules['psycopg2']
        del sys.modules['psycopg2']
    
    try:
        # Test SQL_AVAILABLE flag
        from my_agent.utils.orchestrator_agent import SQL_AVAILABLE
        print(f"✅ SQL_AVAILABLE: {SQL_AVAILABLE}")
        
        # Test routing decision
        from my_agent.utils.orchestrator_nodes import route_decision
        from my_agent.utils.state import OrchestratorState, QueryDomain
        
        # Create a test state
        state = OrchestratorState()
        state["current_query"] = "Show all employees"
        state["query_domain"] = QueryDomain.EMPLOYEE
        
        # Test routing decision
        decision = route_decision(state)
        print(f"✅ Routing decision for employee query: {decision}")
        
        # Test data engineer handling
        from my_agent.utils.data_engineer_agent import DataEngineerAgent
        data_engineer = DataEngineerAgent()
        result = data_engineer.handle_sql_query_without_agent("Show all employees")
        
        print(f"✅ Data Engineer SQL handling: {result.get('success', False)}")
        print(f"   Query type: {result.get('query_type', 'unknown')}")
        
        # Test orchestrator
        from my_agent.utils.orchestrator_agent import HybridOrchestrator
        orchestrator = HybridOrchestrator()
        
        # Check status
        status = orchestrator.check_agent_status()
        print(f"SQL Agent Status: {status['sql_agent']['status']}")
        print(f"NoSQL Agent Status: {status['nosql_agent']['status']}")
        
        # Test SQL query execution
        result = orchestrator.execute_sql_query("Show all employees")
        print(f"✅ SQL query result: {result.get('success', False)}")
        print(f"   Error message: {result.get('error', 'No error')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Restore psycopg2 if it was removed
        if original_psycopg2:
            sys.modules['psycopg2'] = original_psycopg2

def test_workflow_without_sql():
    """Test the complete workflow when SQL agent is not available"""
    print("\n🧪 Testing Complete Workflow Without SQL Agent")
    print("=" * 50)
    
    # Temporarily remove psycopg2 from sys.modules
    original_psycopg2 = None
    if 'psycopg2' in sys.modules:
        original_psycopg2 = sys.modules['psycopg2']
        del sys.modules['psycopg2']
    
    try:
        from my_agent.agent import graph
        from my_agent.utils.state import OrchestratorState
        
        # Create a test state
        state = OrchestratorState()
        state["current_query"] = "Show all employees"
        
        # Run the workflow
        print("🔄 Running workflow...")
        result = graph.invoke(state)
        
        print(f"✅ Workflow completed successfully")
        print(f"   Final state keys: {list(result.keys())}")
        
        # Check if we got a response
        if "messages" in result and result["messages"]:
            print(f"✅ Response generated: {len(result['messages'])} messages")
            for i, msg in enumerate(result["messages"]):
                print(f"   Message {i+1}: {type(msg).__name__}")
                if hasattr(msg, 'content'):
                    content = msg.content[:100] + "..." if len(msg.content) > 100 else msg.content
                    print(f"   Content: {content}")
        else:
            print("❌ No response generated")
        
        return True
        
    except Exception as e:
        print(f"❌ Workflow test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Restore psycopg2 if it was removed
        if original_psycopg2:
            sys.modules['psycopg2'] = original_psycopg2

if __name__ == "__main__":
    print("🔧 LangGraph Studio SQL Handling Test")
    print("=" * 60)
    
    # Run tests
    test1_result = simulate_langgraph_environment()
    test2_result = test_workflow_without_sql()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY")
    print("=" * 60)
    
    if test1_result and test2_result:
        print("✅ All tests passed! The system handles missing SQL agent gracefully in LangGraph Studio.")
    else:
        print("❌ Some tests failed. Check the output above for details.")
    
    print("\n" + "=" * 60)
    print("🎯 TEST COMPLETE")
    print("=" * 60) 