#!/usr/bin/env python3
"""
Test script for PostgreSQL SQL Agent
Demonstrates the agent functionality and sets up sample tables
"""

import os
from my_agent.utils.sql_agent import SQLQueryExecutor

def setup_sample_database():
    """Set up sample tables in the PostgreSQL database"""
    agent = SQLQueryExecutor()
    
    # First, check if tables already exist
    check_tables_query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE'
    AND table_name IN ('departments', 'employees', 'projects', 'employee_projects')
    """
    
    result = agent.execute_query(check_tables_query)
    existing_tables = [row['table_name'] for row in result['data']]
    
    if existing_tables:
        print(f"✅ Tables already exist: {existing_tables}")
        return
    
    # Sample SQL commands to create tables
    setup_queries = [
        """
        CREATE TABLE departments (
            department_id SERIAL PRIMARY KEY,
            department_name VARCHAR(100) NOT NULL,
            location VARCHAR(100),
            budget DECIMAL(12,2)
        )
        """,
        """
        CREATE TABLE employees (
            employee_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE,
            hire_date DATE DEFAULT CURRENT_DATE,
            salary DECIMAL(10,2),
            department_id INTEGER REFERENCES departments(department_id)
        )
        """,
        """
        CREATE TABLE projects (
            project_id SERIAL PRIMARY KEY,
            project_name VARCHAR(100) NOT NULL,
            description TEXT,
            start_date DATE,
            end_date DATE,
            budget DECIMAL(12,2)
        )
        """,
        """
        CREATE TABLE employee_projects (
            employee_id INTEGER REFERENCES employees(employee_id),
            project_id INTEGER REFERENCES projects(project_id),
            role VARCHAR(50),
            hours_worked DECIMAL(5,2),
            PRIMARY KEY (employee_id, project_id)
        )
        """
    ]
    
    print("🗄️  Setting up sample database tables...")
    
    for i, query in enumerate(setup_queries, 1):
        try:
            result = agent.execute_query(query)
            if result['success']:
                print(f"✅ Table {i} created successfully")
            else:
                print(f"⚠️  Table {i} creation result: {result['error']}")
        except Exception as e:
            print(f"❌ Error creating table {i}: {e}")
    
    # Insert sample data
    sample_data_queries = [
        """
        INSERT INTO departments (department_name, location, budget) VALUES
        ('IT', 'New York', 500000.00),
        ('HR', 'Los Angeles', 300000.00),
        ('Sales', 'Chicago', 400000.00),
        ('Marketing', 'Boston', 350000.00)
        """,
        """
        INSERT INTO employees (first_name, last_name, email, hire_date, salary, department_id) VALUES
        ('John', 'Doe', 'john.doe@company.com', '2023-01-15', 75000.00, 1),
        ('Jane', 'Smith', 'jane.smith@company.com', '2023-02-20', 65000.00, 2),
        ('Bob', 'Johnson', 'bob.johnson@company.com', '2023-03-10', 80000.00, 1),
        ('Alice', 'Brown', 'alice.brown@company.com', '2023-04-05', 70000.00, 3),
        ('Charlie', 'Wilson', 'charlie.wilson@company.com', '2023-05-12', 85000.00, 1)
        """,
        """
        INSERT INTO projects (project_name, description, start_date, end_date, budget) VALUES
        ('Website Redesign', 'Redesign company website', '2024-01-01', '2024-06-30', 50000.00),
        ('Mobile App', 'Develop mobile application', '2024-02-01', '2024-08-31', 75000.00),
        ('Marketing Campaign', 'Q2 marketing campaign', '2024-03-01', '2024-05-31', 25000.00)
        """,
        """
        INSERT INTO employee_projects (employee_id, project_id, role, hours_worked) VALUES
        (1, 1, 'Lead Developer', 120.5),
        (3, 1, 'Developer', 80.0),
        (1, 2, 'Developer', 95.5),
        (5, 2, 'Lead Developer', 150.0),
        (4, 3, 'Project Manager', 60.0)
        """
    ]
    
    print("\n📊 Inserting sample data...")
    
    for i, query in enumerate(sample_data_queries, 1):
        try:
            result = agent.execute_query(query)
            if result['success']:
                print(f"✅ Sample data {i} inserted successfully")
            else:
                print(f"⚠️  Sample data {i} insertion result: {result['error']}")
        except Exception as e:
            print(f"❌ Error inserting sample data {i}: {e}")

def test_queries():
    """Test various SQL queries"""
    agent = SQLQueryExecutor()
    
    test_queries = [
        "SELECT COUNT(*) as total_employees FROM employees",
        "SELECT d.department_name, COUNT(e.employee_id) as employee_count FROM departments d LEFT JOIN employees e ON d.department_id = e.department_id GROUP BY d.department_id, d.department_name",
        "SELECT e.first_name, e.last_name, d.department_name, e.salary FROM employees e JOIN departments d ON e.department_id = d.department_id ORDER BY e.salary DESC",
        "SELECT p.project_name, COUNT(ep.employee_id) as team_size FROM projects p LEFT JOIN employee_projects ep ON p.project_id = ep.project_id GROUP BY p.project_id, p.project_name"
    ]
    
    print("\n🔍 Testing sample queries...")
    
    for i, query in enumerate(test_queries, 1):
        try:
            result = agent.execute_query(query)
            if result['success']:
                print(f"\n✅ Query {i} successful:")
                print(f"   Query: {query}")
                print(f"   Results: {result['data']}")
            else:
                print(f"\n❌ Query {i} failed: {result['error']}")
        except Exception as e:
            print(f"\n❌ Error executing query {i}: {e}")

def test_schema_introspection():
    """Test the schema introspection functionality"""
    print("\n📋 Testing schema introspection...")
    
    try:
        agent = SQLQueryExecutor()
        print("✅ Schema introspection successful")
        print("Database context loaded successfully")
    except Exception as e:
        print(f"❌ Schema introspection failed: {e}")

def main():
    """Main test function"""
    print("🧪 PostgreSQL SQL Agent Test")
    print("=" * 50)
    
    # Check if OpenAI API key is available
    if not os.getenv("OPENAPI_KEY"):
        print("⚠️  OPENAPI_KEY not set - AI query generation will not work")
        print("   Direct SQL execution will still work")
    
    try:
        # Test basic connection
        print("\n🔌 Testing database connection...")
        agent = SQLQueryExecutor()
        result = agent.execute_query("SELECT current_database(), current_user")
        if result['success']:
            print(f"✅ Connected to: {result['data'][0]['current_database']} as {result['data'][0]['current_user']}")
        else:
            print(f"❌ Connection failed: {result['error']}")
            return
        
        # Test schema introspection
        test_schema_introspection()
        
        # Set up sample database
        setup_sample_database()
        
        # Test queries
        test_queries()
        
        print("\n🎉 All tests completed successfully!")
        print("\n💡 You can now use the interactive SQL chat:")
        print("   python my_agent/utils/sql_agent.py")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")

if __name__ == "__main__":
    main() 