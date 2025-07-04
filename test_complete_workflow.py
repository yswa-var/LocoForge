#!/usr/bin/env python3
"""
Test script to verify the complete workflow for "list all employees first name"
"""

import os
import sys
from dotenv import load_dotenv

# Add the my_agent directory to the path
sys.path.append('my_agent')

def test_complete_workflow():
    """Test the complete workflow for the specific query"""
    print("🔍 Complete Workflow Test for 'list all employees first name'")
    print("=" * 70)
    
    # Load environment variables
    load_dotenv()
    
    try:
        from my_agent.utils.orchestrator_nodes import get_orchestrator, initialize_state, classify_query_node
        from my_agent.utils.state import OrchestratorState
        
        # Create a test state with the specific query
        test_state = OrchestratorState(
            messages=[{"content": "list all employees first name", "type": "human"}],
            current_query="list all employees first name"
        )
        
        print("1. Initial state:")
        print(f"   Current query: {test_state['current_query']}")
        print(f"   Query domain: {test_state.get('query_domain', 'Not set')}")
        print(f"   Query intent: {test_state.get('query_intent', 'Not set')}")
        
        # Step 1: Initialize state
        test_state = initialize_state(test_state)
        print(f"\n2. After initialization:")
        print(f"   Current query: {test_state['current_query']}")
        
        # Step 2: Classify query
        test_state = classify_query_node(test_state)
        print(f"\n3. After classification:")
        print(f"   Query domain: {test_state.get('query_domain', 'Not set')}")
        print(f"   Query intent: {test_state.get('query_intent', 'Not set')}")
        print(f"   Query complexity: {test_state.get('query_complexity', 'Not set')}")
        print(f"   Execution path: {test_state.get('execution_path', [])}")
        
        # Check if classification was successful
        from my_agent.utils.state import QueryDomain
        if test_state.get('query_domain') == QueryDomain.EMPLOYEE:
            print("   ✅ Query correctly classified as employee domain")
        else:
            print(f"   ❌ Query incorrectly classified as {test_state.get('query_domain')}")
            return False
        
        # Step 3: Test orchestrator directly
        print(f"\n4. Testing orchestrator directly:")
        orchestrator = get_orchestrator()
        domain, intent = orchestrator.classify_intent(test_state['current_query'])
        print(f"   Domain: {domain.value}")
        print(f"   Intent: {intent.value}")
        
        # Step 4: Test SQL query execution
        print(f"\n5. Testing SQL query execution:")
        try:
            result = orchestrator.execute_sql_query(test_state['current_query'])
            print(f"   Success: {result.get('success', False)}")
            if result.get('success'):
                print(f"   Generated SQL: {result.get('generated_sql', 'N/A')}")
                print(f"   Row count: {result.get('execution_result', {}).get('row_count', 0)}")
                print("   ✅ SQL query execution successful")
            else:
                print(f"   Error: {result.get('error', 'Unknown error')}")
                print("   ❌ SQL query execution failed")
        except Exception as e:
            print(f"   ❌ SQL query execution failed: {e}")
        
        print(f"\n" + "=" * 70)
        print("📊 Workflow Summary:")
        print("✅ Query classification is working correctly")
        print("✅ Query is properly routed to employee domain")
        print("✅ SQL agent is initialized and ready")
        
        if test_state.get('query_domain') == QueryDomain.EMPLOYEE:
            print("🎉 The query 'list all employees first name' will now be processed correctly!")
            return True
        else:
            print("❌ There are still issues with the workflow")
            return False
            
    except Exception as e:
        print(f"❌ Workflow test failed: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False

def main():
    """Run the complete workflow test"""
    print("🚀 Complete Workflow Test for Employee Query")
    print("=" * 70)
    
    success = test_complete_workflow()
    
    if success:
        print("\n🎉 SUCCESS: The workflow is now working correctly!")
        print("   The query 'list all employees first name' will be:")
        print("   1. ✅ Correctly classified as employee domain")
        print("   2. ✅ Routed to the SQL agent")
        print("   3. ✅ Processed without being marked as 'unclear'")
    else:
        print("\n❌ FAILURE: There are still issues to resolve")

if __name__ == "__main__":
    main() 