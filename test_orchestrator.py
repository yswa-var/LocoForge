"""
Test interface for the Hybrid Orchestrator
"""

from typing import Dict, Any
from my_agent.agent import graph
from my_agent.utils.state import OrchestratorState
from langchain_core.messages import HumanMessage

def initialize_state(query: str) -> OrchestratorState:
    """Initialize state for a new query"""
    return OrchestratorState(
        messages=[HumanMessage(content=query)],
        current_query=query,
        query_domain=None,
        query_intent=None,
        sub_queries={},
        sql_results=None,
        nosql_results=None,
        combined_results=None,
        context_history=[],
        execution_path=[],
        error_message=None
    )

def process_query(query: str) -> Dict[str, Any]:
    """Process a query through the orchestrator workflow"""
    # Initialize state
    initial_state = initialize_state(query)
    
    # Execute workflow
    final_state = graph.invoke(initial_state)
    
    return {
        "query": query,
        "domain": final_state["query_domain"].value if final_state["query_domain"] else None,
        "intent": final_state["query_intent"].value if final_state["query_intent"] else None,
        "execution_path": final_state["execution_path"],
        "results": final_state["combined_results"],
        "error": final_state.get("error_message"),
        "context_history": final_state["context_history"]
    }

def test_orchestrator():
    """Test the orchestrator with sample queries"""
    test_queries = [
        "Show me all employees",
        "List all products",
        "Show departments that ordered organic products"
    ]
    
    print("🧪 Testing Hybrid Orchestrator\n")
    
    for query in test_queries:
        print(f"🔍 Query: {query}")
        try:
            result = process_query(query)
            print(f"📊 Domain: {result['domain']}")
            print(f"🎯 Intent: {result['intent']}")
            print(f"🛤️  Path: {' → '.join(result['execution_path'])}")
            
            if result['error']:
                print(f"❌ Error: {result['error']}")
            else:
                results = result['results']
                if results and results.get('success', False):
                    print(f"✅ Success: Query executed successfully")
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
                    print(f"❌ Query failed: {results.get('error', 'Unknown error') if results else 'No results'}")
            
            print("-" * 50)
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            print("-" * 50)

def test_detailed_output():
    """Test with detailed JSON output for debugging"""
    query = "Show me all employees"
    print(f"🔍 Detailed test for: {query}")
    
    try:
        result = process_query(query)
        print("📊 Full Result:")
        import json
        print(json.dumps(result, indent=2, default=str))
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    test_orchestrator()
    print("\n" + "="*50)
    test_detailed_output() 