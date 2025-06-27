#!/usr/bin/env python3
"""
Example usage of SQL Query Executor (OpenAI)
Shows how to use the SQL agent programmatically
"""

import json
from my_agent.utils.sql_agent import SQLQueryExecutor

def main():
    """Example usage of the SQL Query Executor"""
    
    # Initialize the SQL agent
    agent = SQLQueryExecutor()
    
    # Example 1: Simple query
    print("🔍 Example 1: Simple query")
    result1 = agent.generate_and_execute_query("Show all employees")
    print(f"Generated SQL: {result1['generated_sql']}")
    print(f"Rows returned: {result1['execution_result']['row_count']}")
    print()
    
    # Example 2: Complex query with JOIN
    print("🔍 Example 2: Complex query with JOIN")
    result2 = agent.generate_and_execute_query("Show employees with their managers and departments")
    print(f"Generated SQL: {result2['generated_sql']}")
    print(f"Rows returned: {result2['execution_result']['row_count']}")
    print()
    
    # Example 3: Aggregation query
    print("🔍 Example 3: Aggregation query")
    result3 = agent.generate_and_execute_query("Show total salary by department")
    print(f"Generated SQL: {result3['generated_sql']}")
    print(f"Rows returned: {result3['execution_result']['row_count']}")
    if result3['execution_result']['success']:
        print("Data:")
        for row in result3['execution_result']['data']:
            print(f"  {row}")
    print()
    
    # Example 4: Direct SQL execution
    print("🔍 Example 4: Direct SQL execution")
    direct_result = agent.execute_query("SELECT COUNT(*) as total_employees FROM employees")
    print(f"Direct SQL: SELECT COUNT(*) as total_employees FROM employees")
    print(f"Result: {direct_result['data']}")
    print()
    
    # Example 5: Get full JSON structure
    print("🔍 Example 5: Full JSON structure")
    full_result = agent.generate_and_execute_query("Find the highest paid employee")
    print("Complete JSON response:")
    print(json.dumps(full_result, indent=2, default=str))

if __name__ == "__main__":
    main() 