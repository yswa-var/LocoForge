#!/usr/bin/env python3
"""
Test script specifically for LangGraph Studio environment
"""

import os
import sys
from dotenv import load_dotenv

def test_langgraph_studio_setup():
    """Test the setup specifically for LangGraph Studio"""
    print("🧪 LANGGRAPH STUDIO SETUP TEST")
    print("=" * 60)
    
    # Force load environment variables
    load_dotenv()
    
    # Check environment variables
    print("1️⃣  Environment Variables:")
    mongo_db = os.getenv("MONGO_DB")
    openai_key = os.getenv("OPENAI_API_KEY")
    sql_db = os.getenv("SQL_DB")
    
    print(f"   MONGO_DB: {mongo_db}")
    print(f"   OPENAI_API_KEY: {'SET' if openai_key else 'NOT SET'}")
    print(f"   SQL_DB: {sql_db}")
    
    # Test orchestrator nodes
    print("\n2️⃣  Testing Orchestrator Nodes:")
    try:
        from my_agent.utils.orchestrator_nodes import get_orchestrator, check_orchestrator_status, reset_orchestrator
        
        # Test status check
        print("   Checking orchestrator status...")
        status = check_orchestrator_status()
        print(f"   Status: {status}")
        
        # Test orchestrator initialization
        print("   Getting orchestrator instance...")
        orchestrator = get_orchestrator()
        print(f"   Orchestrator: {orchestrator}")
        
        # Test NoSQL agent
        if orchestrator.nosql_agent:
            print("   ✅ NoSQL agent is available")
            
            # Test a query
            print("   Testing NoSQL query...")
            result = orchestrator.execute_nosql_query("Show products by category")
            print(f"   Query result: {result.get('success', False)}")
            
            if result.get("success"):
                print("   ✅ NoSQL query successful")
            else:
                print(f"   ❌ NoSQL query failed: {result.get('error', 'Unknown error')}")
        else:
            print("   ❌ NoSQL agent is not available")
        
        # Test workflow
        print("\n3️⃣  Testing Workflow:")
        try:
            from my_agent.agent import graph
            from my_agent.utils.state import OrchestratorState
            
            # Create test state
            state = OrchestratorState(
                current_query="Show products by category",
                query_domain=None,
                query_intent=None,
                sub_queries={},
                sql_results=None,
                nosql_results=None,
                combined_results=None,
                context_history=[],
                execution_path=[],
                error_message=None,
                messages=[{"content": "Show products by category", "type": "human"}]
            )
            
            print("   Executing workflow...")
            result = graph.invoke(state)
            
            if result.get("error_message"):
                print(f"   ❌ Workflow error: {result['error_message']}")
            else:
                print("   ✅ Workflow executed successfully")
                print(f"   🛤️  Path: {' → '.join(result.get('execution_path', []))}")
                
                combined_results = result.get("combined_results", {})
                if combined_results.get("success"):
                    print(f"   📊 Success: {combined_results.get('success')}")
                    print(f"   📦 Data sources: {combined_results.get('data_sources', [])}")
                else:
                    print(f"   ❌ Results failed: {combined_results.get('error', 'Unknown error')}")
            
        except Exception as e:
            print(f"   ❌ Workflow test failed: {e}")
            import traceback
            traceback.print_exc()
        
        # Test reset functionality
        print("\n4️⃣  Testing Reset Functionality:")
        try:
            reset_orchestrator()
            print("   ✅ Reset functionality works")
        except Exception as e:
            print(f"   ❌ Reset failed: {e}")
        
    except Exception as e:
        print(f"   ❌ Orchestrator nodes test failed: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("🎯 LANGGRAPH STUDIO TEST COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    test_langgraph_studio_setup() 