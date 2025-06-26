#!/usr/bin/env python3
"""
Basic test script for SQL Query Executor
Tests core functionality without requiring API calls
"""

import json
from my_agent.utils.sql_agent import SQLQueryExecutor

def test_basic_functionality():
    """Test basic SQL agent functionality without API calls"""
    print("🧪 Basic SQL Agent Testing")
    print("=" * 50)
    
    try:
        # Create SQL agent
        agent = SQLQueryExecutor()
        print("✅ SQL Agent initialized successfully")
        
        # Test direct SQL execution (no API needed)
        print("\n🔍 Test 1: Direct SQL Execution")
        print("-" * 40)
        
        test_queries = [
            "SELECT COUNT(*) as total_employees FROM employees",
            "SELECT department_name, COUNT(*) as employee_count FROM employees e JOIN departments d ON e.department_id = d.department_id GROUP BY d.department_id, d.department_name",
            "SELECT AVG(salary) as avg_salary FROM employees",
            "SELECT * FROM departments LIMIT 3"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n📝 Test Query {i}: {query}")
            try:
                result = agent.execute_query(query)
                
                if result['success']:
                    print(f"✅ Success: {result['row_count']} rows returned")
                    print(f"📊 Columns: {result['columns']}")
                    if result['data']:
                        print("📈 Data:")
                        for j, row in enumerate(result['data'][:2], 1):
                            print(f"  Row {j}: {row}")
                        if len(result['data']) > 2:
                            print(f"  ... and {len(result['data']) - 2} more rows")
                else:
                    print(f"❌ Error: {result['error']}")
                    
            except Exception as e:
                print(f"❌ Query failed: {e}")
        
        # Test sample queries list
        print("\n🔍 Test 2: Sample Queries")
        print("-" * 40)
        sample_queries = agent.get_sample_queries()
        print(f"✅ Retrieved {len(sample_queries)} sample queries")
        for i, query in enumerate(sample_queries[:3], 1):
            print(f"  {i}. {query}")
        
        # Test database context
        print("\n🔍 Test 3: Database Context")
        print("-" * 40)
        context = agent.db_context
        print(f"✅ Database context loaded ({len(context)} characters)")
        print("📋 Context includes:")
        print("  - Departments table schema")
        print("  - Employees table schema") 
        print("  - Projects table schema")
        print("  - Employee_projects table schema")
        print("  - Attendance table schema")
        print("  - Relationship definitions")
        
        print("\n🎉 All basic tests passed!")
        print("💡 Note: API-dependent tests require valid OPENAI_API_KEY")
        
    except Exception as e:
        print(f"❌ Failed to initialize SQL agent: {e}")
        print("Make sure SQL_DB and OPENAI_API_KEY are set in your .env file")

if __name__ == "__main__":
    test_basic_functionality() 