#!/usr/bin/env python3
"""
Interactive test script for LangGraph workflow
"""

import asyncio
from my_agent.agent import graph

async def interactive_test():
    """Interactive test of the orchestrator workflow"""
    
    print("🚀 LangGraph Orchestrator Interactive Test")
    print("=" * 60)
    print("Type 'quit' to exit")
    print()
    
    while True:
        try:
            # Get user input
            query = input("🤖 Enter your query: ").strip()
            
            if query.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
            
            if not query:
                print("❌ Please enter a query")
                continue
            
            # Create initial state
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
            
            print(f"\n🔄 Processing: {query}")
            print("-" * 40)
            
            # Run the graph
            result = await graph.ainvoke(initial_state)
            
            # Show results
            print(f"✅ Domain: {result.get('query_domain')}")
            print(f"📊 Path: {result.get('execution_path')}")
            
            # Check if successful
            if result.get("combined_results", {}).get("success"):
                print("🎉 Query executed successfully!")
                
                # Show data summary
                combined = result["combined_results"]
                if "sql_data" in combined and combined["sql_data"].get("success"):
                    sql_data = combined["sql_data"]
                    print(f"📊 SQL Results: {sql_data.get('row_count', 0)} rows")
                
                if "nosql_data" in combined and combined["nosql_data"].get("success"):
                    nosql_data = combined["nosql_data"]
                    print(f"🎬 NoSQL Results: {nosql_data.get('row_count', 0)} rows")
                    
                    # Show sample data
                    if nosql_data.get("data"):
                        print("📋 Sample results:")
                        for i, item in enumerate(nosql_data["data"][:3]):
                            if isinstance(item, dict):
                                title = item.get('title', 'Unknown')
                                year = item.get('year', 'Unknown')
                                print(f"  {i+1}. {title} ({year})")
                
                # Show final response
                if result.get("messages"):
                    final_message = result["messages"][-1]
                    if hasattr(final_message, 'content'):
                        print(f"\n🤖 Response:\n{final_message.content}")
                    else:
                        print(f"\n🤖 Response:\n{final_message}")
            else:
                print("❌ Query failed")
                error = result.get("combined_results", {}).get("error", "Unknown error")
                print(f"Error: {error}")
            
            print("\n" + "="*60 + "\n")
            
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            print("Please try again")

if __name__ == "__main__":
    asyncio.run(interactive_test()) 