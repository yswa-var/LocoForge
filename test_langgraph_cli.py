#!/usr/bin/env python3
"""
Test script for LangGraph CLI workflow
"""

import asyncio
from my_agent.agent import graph
from my_agent.utils.state import OrchestratorState

async def test_workflow():
    """Test the orchestrator workflow with a movie query"""
    
    # Test query
    test_query = "get movie info titled The Shawshank Redemption"
    
    # Create initial state
    initial_state = {
        "messages": [
            {
                "type": "human",
                "content": test_query
            }
        ],
        "current_query": test_query,
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
    
    print(f"🧪 Testing workflow with query: {test_query}")
    print("=" * 60)
    
    try:
        # Run the graph
        result = await graph.ainvoke(initial_state)
        
        print("✅ Workflow completed successfully!")
        print(f"📊 Execution path: {result.get('execution_path', [])}")
        print(f"🎯 Query domain: {result.get('query_domain')}")
        
        # Check results
        if result.get("combined_results"):
            combined = result["combined_results"]
            print(f"📈 Success: {combined.get('success', False)}")
            
            if combined.get("success"):
                print("🎉 Query executed successfully!")
                if "nosql_data" in combined:
                    nosql_data = combined["nosql_data"]
                    print(f"📊 NoSQL results: {nosql_data.get('row_count', 0)} rows")
                    if nosql_data.get("data"):
                        print("🎬 Movies found:")
                        for movie in nosql_data["data"][:3]:  # Show first 3
                            print(f"  - {movie.get('title', 'Unknown')} ({movie.get('year', 'Unknown')})")
            else:
                print(f"❌ Error: {combined.get('error', 'Unknown error')}")
        
        # Show final response
        if result.get("messages"):
            final_message = result["messages"][-1]
            if hasattr(final_message, 'content'):
                print(f"\n🤖 Final Response:\n{final_message.content}")
            else:
                print(f"\n🤖 Final Response:\n{final_message}")
        
    except Exception as e:
        print(f"❌ Workflow failed: {e}")
        import traceback
        traceback.print_exc()

async def test_multiple_queries():
    """Test multiple different types of queries"""
    
    test_queries = [
        "get movie info titled The Shawshank Redemption",
        "show all employees",
        "find action movies with high ratings",
        "get employee information"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{'='*60}")
        print(f"🧪 Test {i}: {query}")
        print(f"{'='*60}")
        
        initial_state = {
            "messages": [{"type": "human", "content": query}],
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
        
        try:
            result = await graph.ainvoke(initial_state)
            print(f"✅ Domain: {result.get('query_domain')}")
            print(f"📊 Path: {result.get('execution_path')}")
            
            if result.get("combined_results", {}).get("success"):
                print("✅ Success!")
            else:
                print("❌ Failed")
                
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🚀 Starting LangGraph CLI Tests")
    print("=" * 60)
    
    # Test single query
    asyncio.run(test_workflow())
    
    # Test multiple queries
    print("\n" + "="*60)
    print("🧪 Testing Multiple Queries")
    print("="*60)
    asyncio.run(test_multiple_queries()) 