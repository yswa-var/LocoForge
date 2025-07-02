#!/usr/bin/env python3
"""
Test direct SQL/NoSQL query routing
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from my_agent.utils.orchestrator_nodes import (
    is_direct_sql_query, 
    is_direct_nosql_query,
    classify_query_node,
    initialize_state
)
from my_agent.utils.state import OrchestratorState, QueryDomain

def test_direct_sql_detection():
    """Test SQL query detection"""
    print("Testing SQL query detection...")
    
    # Test cases
    sql_queries = [
        "SELECT * FROM employees",
        "SELECT name, salary FROM employees WHERE department = 'IT'",
        "INSERT INTO employees (name, salary) VALUES ('John', 50000)",
        "UPDATE employees SET salary = 60000 WHERE id = 1",
        "DELETE FROM employees WHERE id = 1",
        "CREATE TABLE test (id INT, name VARCHAR(50))",
        "DROP TABLE test",
        "ALTER TABLE employees ADD COLUMN phone VARCHAR(20)",
        "SHOW TABLES",
        "DESCRIBE employees",
        "EXPLAIN SELECT * FROM employees",
        "USE employee_db",
        "SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id",
        "SELECT * FROM employees WHERE salary > 50000 ORDER BY name",
        "SELECT dept_id, AVG(salary) FROM employees GROUP BY dept_id HAVING AVG(salary) > 50000"
    ]
    
    non_sql_queries = [
        "Show me all employees",
        "What's the weather like?",
        "How many products do we have?",
        "Find employees with high salaries",
        "List all departments"
    ]
    
    print("\nTesting SQL queries (should return True):")
    for query in sql_queries:
        result = is_direct_sql_query(query)
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {query[:50]}...")
    
    print("\nTesting non-SQL queries (should return False):")
    for query in non_sql_queries:
        result = is_direct_sql_query(query)
        status = "✅ PASS" if not result else "❌ FAIL"
        print(f"{status}: {query}")

def test_direct_nosql_detection():
    """Test NoSQL query detection"""
    print("\n\nTesting NoSQL query detection...")
    
    # Test cases
    nosql_queries = [
        "db.products.find()",
        "db.products.findOne({name: 'Apple'})",
        "db.products.aggregate([{$match: {category: 'Fruits'}}])",
        "db.products.insert({name: 'Orange', price: 2.50})",
        "db.products.update({name: 'Apple'}, {$set: {price: 3.00}})",
        "db.products.deleteOne({name: 'Apple'})",
        "db.products.remove({category: 'Expired'})",
        "collection.find({price: {$gt: 10}})",
        "db.products.aggregate([{$group: {_id: '$category', count: {$sum: 1}}}])",
        "db.products.find({$and: [{price: {$lt: 5}}, {stock: {$gt: 10}}]})"
    ]
    
    non_nosql_queries = [
        "Show me all products",
        "What products are in stock?",
        "Find products with low inventory",
        "List all categories",
        "How many products do we have?"
    ]
    
    print("\nTesting NoSQL queries (should return True):")
    for query in nosql_queries:
        result = is_direct_nosql_query(query)
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {query}")
    
    print("\nTesting non-NoSQL queries (should return False):")
    for query in non_nosql_queries:
        result = is_direct_nosql_query(query)
        status = "✅ PASS" if not result else "❌ FAIL"
        print(f"{status}: {query}")

def test_classification_routing():
    """Test that direct SQL queries are properly routed"""
    print("\n\nTesting classification routing...")
    
    # Test direct SQL query
    sql_query = "SELECT * FROM employees WHERE department = 'IT'"
    
    # Create state
    state = OrchestratorState(
        current_query=sql_query,
        messages=[{"type": "human", "content": sql_query}]
    )
    
    # Classify query
    result_state = classify_query_node(state)
    
    print(f"Original query: {sql_query}")
    print(f"Query domain: {result_state.get('query_domain')}")
    print(f"Query intent: {result_state.get('query_intent')}")
    print(f"Query complexity: {result_state.get('query_complexity')}")
    print(f"Execution path: {result_state.get('execution_path')}")
    
    # Check if it was routed to SQL domain
    if result_state.get('query_domain') == QueryDomain.EMPLOYEE:
        print("✅ PASS: Direct SQL query properly routed to SQL domain")
    else:
        print("❌ FAIL: Direct SQL query not routed to SQL domain")
        print(f"Expected: QueryDomain.EMPLOYEE, Got: {result_state.get('query_domain')}")

if __name__ == "__main__":
    test_direct_sql_detection()
    test_direct_nosql_detection()
    test_classification_routing() 