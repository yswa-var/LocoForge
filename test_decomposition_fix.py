#!/usr/bin/env python3
"""
Test script to verify query decomposition fix
"""

import json
from my_agent.agent import graph
from my_agent.utils.state import OrchestratorState
from langchain_core.messages import HumanMessage

def test_decomposition():
    """Test that hybrid queries are properly decomposed"""
    
    # Test query
    test_query = "Find employees with perfect attendance records who placed orders over $100"
    
    print(f"🧪 Testing Query Decomposition")
    print(f"Query: {test_query}")
    print("=" * 80)
    
    # Create initial state
    initial_state = OrchestratorState(
        messages=[HumanMessage(content=test_query)],
        current_query=test_query,
        query_domain=None,
        query_intent=None,
        query_complexity=None,
        sub_queries={},
        sql_results=None,
        nosql_results=None,
        combined_results=None,
        context_history=[],
        execution_path=[],
        error_message=None,
        clarification_suggestions=None,
        data_engineer_response=None
    )
    
    try:
        # Execute the workflow up to decompose_query
        from my_agent.utils.orchestrator_nodes import decompose_query_node, initialize_state
        
        # First, run classify_query to set the domain
        from my_agent.utils.orchestrator_nodes import classify_query_node
        state_after_classify = classify_query_node(initial_state)
        
        print(f"📊 Domain after classification: {state_after_classify['query_domain'].value if state_after_classify['query_domain'] else 'None'}")
        print(f"🎯 Intent after classification: {state_after_classify['query_intent'].value if state_after_classify['query_intent'] else 'None'}")
        
        # Then run decompose_query
        state_after_decompose = decompose_query_node(state_after_classify)
        
        print(f"\n🔍 Sub-queries after decomposition:")
        sub_queries = state_after_decompose.get("sub_queries", {})
        for key, value in sub_queries.items():
            print(f"  {key}: {value}")
        
        # Check if decomposition worked correctly
        if state_after_classify['query_domain'].value == 'hybrid':
            sql_query = sub_queries.get("sql", "")
            nosql_query = sub_queries.get("nosql", "")
            
            if sql_query and nosql_query and sql_query != nosql_query:
                print(f"\n✅ SUCCESS: Queries properly decomposed")
                print(f"   SQL: {sql_query}")
                print(f"   NoSQL: {nosql_query}")
            else:
                print(f"\n❌ FAILED: Queries not properly decomposed")
                print(f"   SQL: {sql_query}")
                print(f"   NoSQL: {nosql_query}")
        else:
            print(f"\n⚠️  Query was not classified as hybrid")
        
    except Exception as e:
        print(f"💥 Exception: {str(e)}")
        import traceback
        print(f"📋 Traceback: {traceback.format_exc()}")

def test_full_workflow():
    """Test the full workflow to see the final results"""
    
    test_query = "Find employees with perfect attendance records who placed orders over $100"
    
    print(f"\n🧪 Testing Full Workflow")
    print(f"Query: {test_query}")
    print("=" * 80)
    
    initial_state = OrchestratorState(
        messages=[HumanMessage(content=test_query)],
        current_query=test_query,
        query_domain=None,
        query_intent=None,
        query_complexity=None,
        sub_queries={},
        sql_results=None,
        nosql_results=None,
        combined_results=None,
        context_history=[],
        execution_path=[],
        error_message=None,
        clarification_suggestions=None,
        data_engineer_response=None
    )
    
    try:
        # Execute the full workflow
        final_state = graph.invoke(initial_state)
        
        print(f"📊 Final Domain: {final_state['query_domain'].value if final_state['query_domain'] else 'None'}")
        print(f"🛤️  Execution Path: {' → '.join(final_state['execution_path'])}")
        
        # Check sub-queries in final state
        sub_queries = final_state.get("sub_queries", {})
        print(f"\n🔍 Final Sub-queries:")
        for key, value in sub_queries.items():
            print(f"  {key}: {value}")
        
        # Check results
        results = final_state.get("combined_results")
        if results and results.get("success", False):
            print(f"\n✅ Query executed successfully")
            print(f"📊 Data sources: {results.get('data_sources', [])}")
            
            if 'sql_data' in results:
                sql_data = results['sql_data']
                if sql_data.get('success', False):
                    print(f"🗄️  SQL: {sql_data.get('row_count', 0)} rows")
                else:
                    print(f"❌ SQL Error: {sql_data.get('error', 'Unknown error')}")
            
            if 'nosql_data' in results:
                nosql_data = results['nosql_data']
                if nosql_data.get('success', False):
                    print(f"📦 NoSQL: {nosql_data.get('row_count', 0)} rows")
                else:
                    print(f"❌ NoSQL Error: {nosql_data.get('error', 'Unknown error')}")
        else:
            print(f"\n❌ Query failed: {results.get('error', 'Unknown error') if results else 'No results'}")
        
    except Exception as e:
        print(f"💥 Exception: {str(e)}")
        import traceback
        print(f"📋 Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    print("🚀 Query Decomposition Fix Test")
    print("=" * 80)
    
    # Test decomposition
    test_decomposition()
    
    # Test full workflow
    test_full_workflow()
    
    print("\n🎉 Testing Complete!") 