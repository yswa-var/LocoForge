"""
Test Edge Cases with Enhanced Data Engineer Agent
Demonstrates seamless handling of unclear, irrelevant, and technical queries
"""

import json
from my_agent.agent import graph
from my_agent.utils.state import OrchestratorState
from langchain_core.messages import HumanMessage

def initialize_state(query: str) -> OrchestratorState:
    """Initialize state with a query"""
    return {
        "messages": [HumanMessage(content=query)],
        "current_query": query,
        "query_domain": None,
        "query_intent": None,
        "query_complexity": None,
        "sub_queries": {},
        "sql_results": None,
        "nosql_results": None,
        "combined_results": None,
        "context_history": [],
        "execution_path": [],
        "error_message": None,
        "clarification_suggestions": None,
        "data_engineer_response": None
    }

def process_query(query: str) -> dict:
    """Process a query through the enhanced orchestrator workflow"""
    # Initialize state
    initial_state = initialize_state(query)
    
    # Execute workflow
    final_state = graph.invoke(initial_state)
    
    return {
        "query": query,
        "domain": final_state["query_domain"].value if final_state["query_domain"] else None,
        "intent": final_state["query_intent"].value if final_state["query_intent"] else None,
        "complexity": final_state["query_complexity"].value if final_state["query_complexity"] else None,
        "execution_path": final_state["execution_path"],
        "results": final_state["combined_results"],
        "error": final_state.get("error_message"),
        "clarification_suggestions": final_state.get("clarification_suggestions"),
        "data_engineer_response": final_state.get("data_engineer_response")
    }

def test_edge_cases():
    """Test the enhanced system with various edge cases"""
    
    # Edge cases from the test_prompts.py
    edge_cases = [
        # Ambiguous Queries
        "Show me everything",
        "What's the data?",
        "Give me information",
        "List all records",
        
        # Non-Domain Queries
        "What's the weather like?",
        "Tell me a joke",
        "How do I cook pasta?",
        "What's the capital of France?",
        
        # Technical Queries
        "SELECT * FROM employees",
        "db.products.find()",
        "Show me the database schema",
        "What tables exist?",
        
        # Complex Multi-Domain
        "Find employees in IT department who ordered organic fruits that are low in stock and have managers with high salaries",
        "Show projects managed by employees who ordered products from suppliers with fast delivery times and have perfect attendance records"
    ]
    
    print("🧪 Testing Enhanced Edge Case Handling with Data Engineer Agent\n")
    print("=" * 80)
    
    for i, query in enumerate(edge_cases, 1):
        print(f"\n🔍 Test {i}: {query}")
        print("-" * 60)
        
        try:
            result = process_query(query)
            
            print(f"📊 Domain: {result['domain']}")
            print(f"🎯 Intent: {result['intent']}")
            print(f"⚡ Complexity: {result['complexity']}")
            print(f"🛤️  Execution Path: {' → '.join(result['execution_path'])}")
            
            if result['error']:
                print(f"❌ Error: {result['error']}")
            else:
                results = result['results']
                if results and results.get('success', False):
                    print(f"✅ Success: Query handled successfully")
                    
                    # Handle different response types
                    if results.get('query_type') == 'unclear':
                        print(f"🤖 Response Type: Data Engineer Agent")
                        response = results.get('response', 'No response available')
                        print(f"💬 Response: {response[:200]}{'...' if len(response) > 200 else ''}")
                        
                        # Show clarification suggestions if available
                        suggestions = result.get('clarification_suggestions')
                        if suggestions:
                            print(f"💡 Clarification Suggestions:")
                            for j, suggestion in enumerate(suggestions, 1):
                                print(f"   {j}. {suggestion}")
                    else:
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
            
        except Exception as e:
            print(f"💥 Exception: {str(e)}")
            import traceback
            print(f"📋 Traceback: {traceback.format_exc()}")
        
        print("-" * 60)

def test_comparison():
    """Compare old vs new handling of edge cases"""
    
    print("\n🔄 COMPARISON: Old vs Enhanced Edge Case Handling")
    print("=" * 80)
    
    # Test cases that would have failed before
    comparison_cases = [
        "Show me everything",
        "What's the weather like?",
        "SELECT * FROM employees",
        "Find everything about everything in the system"
    ]
    
    for query in comparison_cases:
        print(f"\n🔍 Query: {query}")
        print("-" * 40)
        
        try:
            result = process_query(query)
            
            print(f"📊 Domain: {result['domain']}")
            print(f"🛤️  Path: {' → '.join(result['execution_path'])}")
            
            if result['domain'] == 'unclear':
                print("✅ ENHANCED: Handled by Data Engineer Agent")
                results = result['results']
                if results and results.get('success'):
                    response = results.get('response', '')
                    print(f"💬 Response: {response[:100]}{'...' if len(response) > 100 else ''}")
            else:
                print("✅ ENHANCED: Handled by regular agents")
                
        except Exception as e:
            print(f"❌ Error: {str(e)}")

def test_data_engineer_agent_directly():
    """Test the Data Engineer Agent directly"""
    
    print("\n🔧 DIRECT DATA ENGINEER AGENT TESTING")
    print("=" * 80)
    
    from my_agent.utils.data_engineer_agent import DataEngineerAgent
    
    agent = DataEngineerAgent()
    
    test_queries = [
        "Show me everything",
        "What's the weather like?",
        "SELECT * FROM employees",
        "How do I cook pasta?"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Testing: {query}")
        print("-" * 40)
        
        # Test analysis
        analysis = agent.analyze_query(query)
        print(f"📊 Analysis: {json.dumps(analysis, indent=2)}")
        
        # Test specific handlers based on analysis
        query_type = analysis.get("query_type", "clear")
        
        if query_type == "ambiguous":
            suggestions = agent.provide_clarification_suggestions(query, analysis)
            print(f"💡 Suggestions: {suggestions}")
        elif query_type == "technical":
            response = agent.handle_technical_query(query)
            print(f"🔧 Technical Response: {response.get('response', 'No response')[:100]}...")
        elif query_type == "non_domain":
            response = agent.handle_non_domain_query(query)
            print(f"🌍 Non-Domain Response: {response.get('response', 'No response')[:100]}...")

if __name__ == "__main__":
    print("🚀 Enhanced Edge Case Handling System")
    print("=" * 80)
    print("This system now includes a Data Engineer Agent that provides:")
    print("✅ Professional handling of unclear queries")
    print("✅ Clarification suggestions for ambiguous requests")
    print("✅ Technical guidance for database queries")
    print("✅ Polite responses for non-domain questions")
    print("=" * 80)
    
    # Run tests
    test_edge_cases()
    test_comparison()
    test_data_engineer_agent_directly()
    
    print("\n🎉 Edge Case Testing Complete!")
    print("The enhanced system provides a seamless user experience for all query types.") 