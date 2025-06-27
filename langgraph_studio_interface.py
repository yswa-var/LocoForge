#!/usr/bin/env python3
"""
Simple interface for LangGraph Studio testing
"""

from my_agent.utils.orchestrator_nodes import get_orchestrator, check_orchestrator_status, reset_orchestrator
from my_agent.agent import graph
from my_agent.utils.state import OrchestratorState

def test_query(query: str):
    """Test a query through the workflow"""
    print(f"🧪 Testing query: {query}")
    print("=" * 50)
    
    # Create state
    state = OrchestratorState(
        current_query=query,
        query_domain=None,
        query_intent=None,
        sub_queries={},
        sql_results=None,
        nosql_results=None,
        combined_results=None,
        context_history=[],
        execution_path=[],
        error_message=None,
        messages=[{"content": query, "type": "human"}]
    )
    
    try:
        # Execute workflow
        result = graph.invoke(state)
        
        # Check for errors
        if result.get("error_message"):
            print(f"❌ Error: {result['error_message']}")
            return result
        
        # Show results
        print(f"✅ Query processed successfully")
        print(f"🛤️  Execution path: {' → '.join(result.get('execution_path', []))}")
        
        combined_results = result.get("combined_results", {})
        if combined_results.get("success"):
            print(f"📊 Success: {combined_results.get('success')}")
            print(f"📦 Data sources: {combined_results.get('data_sources', [])}")
            
            # Show data counts
            sql_data = combined_results.get("sql_data", {})
            nosql_data = combined_results.get("nosql_data", {})
            
            if sql_data.get("success"):
                print(f"🗄️  SQL: {sql_data.get('row_count', 0)} rows")
            if nosql_data.get("success"):
                print(f"📦 NoSQL: {nosql_data.get('row_count', 0)} rows")
                # Show sample data
                data = nosql_data.get("data", [])
                if data:
                    print(f"📋 Sample data: {data[:2]}")  # Show first 2 items
        else:
            print(f"❌ Results failed: {combined_results.get('error', 'Unknown error')}")
        
        return result
        
    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback
        traceback.print_exc()
        return None

def check_status():
    """Check orchestrator status"""
    print("🔍 Checking orchestrator status...")
    status = check_orchestrator_status()
    
    print(f"SQL Agent: {status.get('sql_agent', {}).get('status', 'Unknown')}")
    print(f"NoSQL Agent: {status.get('nosql_agent', {}).get('status', 'Unknown')}")
    
    if 'error' in status:
        print(f"❌ Error: {status['error']}")
    
    return status

def reset():
    """Reset the orchestrator"""
    print("🔄 Resetting orchestrator...")
    reset_orchestrator()
    print("✅ Orchestrator reset complete")

# Example usage functions
def test_warehouse_queries():
    """Test warehouse-related queries"""
    queries = [
        "Show products by category",
        "Find products with low stock",
        "Show high-value orders above $500",
        "List products by category"
    ]
    
    print("🧪 TESTING WAREHOUSE QUERIES")
    print("=" * 60)
    
    for query in queries:
        print(f"\n🔍 Testing: {query}")
        result = test_query(query)
        if result and result.get("error_message"):
            print(f"❌ Failed: {result['error_message']}")
        else:
            print("✅ Success")
        print("-" * 40)

def test_employee_queries():
    """Test employee-related queries"""
    queries = [
        "Show me all employees",
        "Find employees in the IT department",
        "Show employees with salary above $80000"
    ]
    
    print("🧪 TESTING EMPLOYEE QUERIES")
    print("=" * 60)
    
    for query in queries:
        print(f"\n🔍 Testing: {query}")
        result = test_query(query)
        if result and result.get("error_message"):
            print(f"❌ Failed: {result['error_message']}")
        else:
            print("✅ Success")
        print("-" * 40)

if __name__ == "__main__":
    print("🎯 LANGGRAPH STUDIO INTERFACE")
    print("=" * 60)
    
    # Check status first
    check_status()
    
    # Test a specific query
    print("\n" + "=" * 60)
    test_query("Show products by category")
    
    print("\n" + "=" * 60)
    print("🎯 INTERFACE TEST COMPLETE")
    print("=" * 60) 