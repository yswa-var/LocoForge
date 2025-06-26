#!/usr/bin/env python3
"""
Diagnostic script to check orchestrator status and identify initialization issues
"""

import os
import sys
from dotenv import load_dotenv

def check_environment():
    """Check environment variables"""
    print("🔍 CHECKING ENVIRONMENT")
    print("=" * 50)
    
    load_dotenv()
    
    env_vars = {
        "MONGO_DB": os.getenv("MONGO_DB"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "SQL_DB": os.getenv("SQL_DB")
    }
    
    for var, value in env_vars.items():
        if value:
            if var == "OPENAI_API_KEY":
                print(f"✅ {var}: SET")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: NOT SET")
    
    return env_vars

def check_imports():
    """Check if required modules can be imported"""
    print("\n📦 CHECKING IMPORTS")
    print("=" * 50)
    
    imports = {
        "pymongo": "MongoDB driver",
        "langchain_openai": "OpenAI integration",
        "my_agent.utils.nosql_agent": "NoSQL agent",
        "my_agent.utils.sql_agent": "SQL agent",
        "my_agent.utils.orchestrator_agent": "Orchestrator"
    }
    
    for module, description in imports.items():
        try:
            __import__(module)
            print(f"✅ {module} ({description})")
        except ImportError as e:
            print(f"❌ {module} ({description}): {e}")
    
    return True

def check_orchestrator():
    """Check orchestrator initialization"""
    print("\n🎯 CHECKING ORCHESTRATOR")
    print("=" * 50)
    
    try:
        from my_agent.utils.orchestrator_agent import HybridOrchestrator
        
        # Create orchestrator
        print("Creating orchestrator...")
        orchestrator = HybridOrchestrator()
        
        # Check status
        status = orchestrator.check_agent_status()
        
        print(f"\nSQL Agent: {status['sql_agent']['status']}")
        print(f"NoSQL Agent: {status['nosql_agent']['status']}")
        
        if not status['nosql_agent']['initialized']:
            print(f"\n❌ NoSQL Agent Error: {status['nosql_agent'].get('error', 'Unknown error')}")
        
        # Test a query if NoSQL agent is available
        if status['nosql_agent']['initialized']:
            print("\n🧪 Testing NoSQL query...")
            result = orchestrator.execute_nosql_query("Show products by category")
            
            if result.get("success") is False:
                print(f"❌ Query failed: {result.get('error', 'Unknown error')}")
            else:
                print("✅ Query successful")
                print(f"📊 Row count: {result.get('execution_result', {}).get('row_count', 0)}")
        
        return orchestrator, status
        
    except Exception as e:
        print(f"❌ Orchestrator initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def check_workflow():
    """Check workflow execution"""
    print("\n🔄 CHECKING WORKFLOW")
    print("=" * 50)
    
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
        
        print("Executing workflow...")
        result = graph.invoke(state)
        
        if result.get("error_message"):
            print(f"❌ Workflow error: {result['error_message']}")
        else:
            print("✅ Workflow executed successfully")
            print(f"🛤️  Path: {' → '.join(result.get('execution_path', []))}")
            
            combined_results = result.get("combined_results", {})
            if combined_results.get("success"):
                print(f"📊 Success: {combined_results.get('success')}")
                print(f"📦 Data sources: {combined_results.get('data_sources', [])}")
            else:
                print(f"❌ Results failed: {combined_results.get('error', 'Unknown error')}")
        
        return result
        
    except Exception as e:
        print(f"❌ Workflow execution failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main diagnostic function"""
    print("🔧 ORCHESTRATOR DIAGNOSTIC TOOL")
    print("=" * 60)
    
    # Check environment
    env_vars = check_environment()
    
    # Check imports
    check_imports()
    
    # Check orchestrator
    orchestrator, status = check_orchestrator()
    
    # Check workflow
    workflow_result = check_workflow()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY")
    print("=" * 60)
    
    if orchestrator and status:
        sql_ok = status['sql_agent']['initialized']
        nosql_ok = status['nosql_agent']['initialized']
        
        if sql_ok and nosql_ok:
            print("✅ Both agents are working correctly")
        elif nosql_ok:
            print("✅ NoSQL agent is working correctly")
        else:
            print("❌ NoSQL agent has issues")
            
        if workflow_result and not workflow_result.get("error_message"):
            print("✅ Workflow is working correctly")
        else:
            print("❌ Workflow has issues")
    
    print("\n" + "=" * 60)
    print("🎯 DIAGNOSTIC COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main() 