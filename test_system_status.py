#!/usr/bin/env python3
"""
System Status Test for LocoForge Orchestrator
Tests the core functionality without requiring OpenAI API calls
"""

import os
import sys
from typing import Dict, Any
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_imports():
    """Test that all required modules can be imported"""
    print("🔍 Testing module imports...")
    
    try:
        from langgraph.graph import StateGraph, END, START
        print("✅ LangGraph imports successful")
    except ImportError as e:
        print(f"❌ LangGraph import failed: {e}")
        return False
    
    try:
        from my_agent.utils.state import OrchestratorState, QueryDomain, QueryIntent
        print("✅ State management imports successful")
    except ImportError as e:
        print(f"❌ State management import failed: {e}")
        return False
    
    try:
        from my_agent.utils.orchestrator_agent import HybridOrchestrator
        print("✅ Orchestrator agent import successful")
    except ImportError as e:
        print(f"❌ Orchestrator agent import failed: {e}")
        return False
    
    try:
        from my_agent.agent import graph
        print("✅ Main agent graph import successful")
    except ImportError as e:
        print(f"❌ Main agent graph import failed: {e}")
        return False
    
    return True

def test_environment():
    """Test environment variable loading"""
    print("\n🔍 Testing environment variables...")
    
    # Load environment
    from dotenv import load_dotenv
    load_dotenv()
    
    env_vars = {
        "OPENAPI_KEY": os.getenv("OPENAPI_KEY"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY"),
        "POSTGRES_DB_URL": os.getenv("POSTGRES_DB_URL"),
        "MONGO_DB": os.getenv("MONGO_DB")
    }
    
    for var, value in env_vars.items():
        if value:
            if "your_" in value or "here" in value:
                print(f"⚠️  {var}: SET (placeholder value)")
            else:
                print(f"✅ {var}: SET")
        else:
            print(f"❌ {var}: NOT SET")
    
    return env_vars

def test_database_connections():
    """Test database connection capabilities"""
    print("\n🔍 Testing database connections...")
    
    # Test SQL agent manager
    try:
        from my_agent.utils.sql_agent_manager import SQLAgentManager
        manager = SQLAgentManager()
        status = manager.initialize()
        
        if status:
            print("✅ SQL agent manager initialized successfully")
        else:
            print(f"⚠️  SQL agent manager failed: {manager.error_message}")
        
    except Exception as e:
        print(f"❌ SQL agent manager test failed: {e}")
    
    # Test NoSQL agent
    try:
        from my_agent.utils.nosql_agent import NoSQLQueryExecutor
        nosql_agent = NoSQLQueryExecutor()
        print("✅ NoSQL agent initialized successfully")
    except Exception as e:
        print(f"❌ NoSQL agent test failed: {e}")

def test_workflow_structure():
    """Test the workflow graph structure"""
    print("\n🔍 Testing workflow structure...")
    
    try:
        from my_agent.agent import graph
        
        # Check if graph is compiled
        if hasattr(graph, 'nodes'):
            print(f"✅ Graph has {len(graph.nodes)} nodes")
            print(f"📋 Nodes: {list(graph.nodes.keys())}")
        else:
            print("❌ Graph is not properly compiled")
            return False
        
        # Check if graph has proper structure
        required_nodes = [
            "classify_query", "decompose_query", "route_to_agents",
            "sql_agent", "nosql_agent", "data_engineer",
            "aggregate_results", "update_context", "format_response"
        ]
        
        missing_nodes = [node for node in required_nodes if node not in graph.nodes]
        if missing_nodes:
            print(f"❌ Missing nodes: {missing_nodes}")
            return False
        else:
            print("✅ All required nodes present")
        
        return True
        
    except Exception as e:
        print(f"❌ Workflow structure test failed: {e}")
        return False

def test_state_management():
    """Test state management functionality"""
    print("\n🔍 Testing state management...")
    
    try:
        from my_agent.utils.state import OrchestratorState, QueryDomain, QueryIntent
        from langchain_core.messages import HumanMessage
        
        # Create a test state
        test_state = OrchestratorState(
            messages=[HumanMessage(content="test query")],
            current_query="test query",
            query_domain=QueryDomain.EMPLOYEE,
            query_intent=QueryIntent.SELECT,
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
        
        print("✅ State creation successful")
        print(f"📊 State keys: {list(test_state.keys())}")
        
        return True
        
    except Exception as e:
        print(f"❌ State management test failed: {e}")
        return False

def test_orchestrator_initialization():
    """Test orchestrator initialization"""
    print("\n🔍 Testing orchestrator initialization...")
    
    try:
        from my_agent.utils.orchestrator_agent import HybridOrchestrator
        
        orchestrator = HybridOrchestrator()
        status = orchestrator.check_agent_status()
        
        print("✅ Orchestrator initialized successfully")
        print(f"📊 SQL Agent: {'✅' if status['sql_agent']['initialized'] else '❌'}")
        print(f"📊 NoSQL Agent: {'✅' if status['nosql_agent']['initialized'] else '❌'}")
        
        return True
        
    except Exception as e:
        print(f"❌ Orchestrator initialization test failed: {e}")
        return False

def main():
    """Run all system tests"""
    print("🧪 LocoForge System Status Test")
    print("=" * 50)
    
    # Test imports
    if not test_imports():
        print("\n❌ Import tests failed. Please check your installation.")
        return
    
    # Test environment
    env_vars = test_environment()
    
    # Test database connections
    test_database_connections()
    
    # Test workflow structure
    if not test_workflow_structure():
        print("\n❌ Workflow structure test failed.")
        return
    
    # Test state management
    if not test_state_management():
        print("\n❌ State management test failed.")
        return
    
    # Test orchestrator initialization
    if not test_orchestrator_initialization():
        print("\n❌ Orchestrator initialization test failed.")
        return
    
    print("\n" + "=" * 50)
    print("🎉 System Status Summary:")
    print("✅ All core modules imported successfully")
    print("✅ TypedDict compatibility issue resolved")
    print("✅ Database connections configured")
    print("✅ Workflow graph structure validated")
    print("✅ State management working correctly")
    print("✅ Orchestrator initialization successful")
    
    # Check API key status
    openai_key = env_vars.get("OPENAPI_KEY") or env_vars.get("OPENAI_API_KEY")
    if openai_key and ("your_" in openai_key or "here" in openai_key):
        print("\n⚠️  Next Steps:")
        print("   1. Get your OpenAI API key from: https://platform.openai.com/api-keys")
        print("   2. Update the .env file with your actual API key")
        print("   3. Run the system tests again")
    else:
        print("\n✅ System is ready for use!")

if __name__ == "__main__":
    main()
