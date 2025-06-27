import sqlite3
import pandas as pd

def test_database_queries():
    """Test various queries on the employee management database"""
    
    conn = sqlite3.connect("employee_management.db")
    
    print("=== EMPLOYEE MANAGEMENT DATABASE - SAMPLE QUERIES ===\n")
    
    # Query 1: Get all departments with employee count
    print("1. Departments with employee count:")
    query1 = """
    SELECT 
        d.department_name,
        d.location,
        d.budget,
        COUNT(e.employee_id) as employee_count
    FROM departments d
    LEFT JOIN employees e ON d.department_id = e.department_id
    GROUP BY d.department_id, d.department_name, d.location, d.budget
    ORDER BY employee_count DESC
    """
    
    df1 = pd.read_sql_query(query1, conn)
    print(df1.to_string(index=False))
    print("\n" + "="*80 + "\n")
    
    # Query 2: Employee salary statistics by department
    print("2. Salary statistics by department:")
    query2 = """
    SELECT 
        d.department_name,
        COUNT(e.employee_id) as employee_count,
        ROUND(AVG(e.salary), 2) as avg_salary,
        MIN(e.salary) as min_salary,
        MAX(e.salary) as max_salary,
        ROUND(SUM(e.salary), 2) as total_salary
    FROM departments d
    LEFT JOIN employees e ON d.department_id = e.department_id
    GROUP BY d.department_id, d.department_name
    ORDER BY avg_salary DESC
    """
    
    df2 = pd.read_sql_query(query2, conn)
    print(df2.to_string(index=False))
    print("\n" + "="*80 + "\n")
    
    # Query 3: Active projects with assigned employees
    print("3. Active projects with assigned employees:")
    query3 = """
    SELECT 
        p.project_name,
        p.description,
        p.budget,
        d.department_name,
        COUNT(ep.employee_id) as assigned_employees,
        SUM(ep.hours_allocated) as total_hours_allocated
    FROM projects p
    JOIN departments d ON p.department_id = d.department_id
    LEFT JOIN employee_projects ep ON p.project_id = ep.project_id
    WHERE p.status = 'active'
    GROUP BY p.project_id, p.project_name, p.description, p.budget, d.department_name
    ORDER BY p.budget DESC
    """
    
    df3 = pd.read_sql_query(query3, conn)
    print(df3.to_string(index=False))
    print("\n" + "="*80 + "\n")
    
    # Query 4: Employee attendance summary (last 7 days)
    print("4. Employee attendance summary (last 7 days):")
    query4 = """
    SELECT 
        e.first_name || ' ' || e.last_name as employee_name,
        d.department_name,
        COUNT(a.attendance_id) as days_present,
        ROUND(AVG(a.hours_worked), 2) as avg_hours_worked,
        ROUND(SUM(a.hours_worked), 2) as total_hours_worked
    FROM employees e
    JOIN departments d ON e.department_id = d.department_id
    LEFT JOIN attendance a ON e.employee_id = a.employee_id
    WHERE a.date >= date('now', '-7 days')
    GROUP BY e.employee_id, e.first_name, e.last_name, d.department_name
    ORDER BY total_hours_worked DESC
    LIMIT 10
    """
    
    df4 = pd.read_sql_query(query4, conn)
    print(df4.to_string(index=False))
    print("\n" + "="*80 + "\n")
    
    # Query 5: Manager hierarchy
    print("5. Manager hierarchy:")
    query5 = """
    SELECT 
        e1.first_name || ' ' || e1.last_name as employee_name,
        e1.position as employee_position,
        d.department_name,
        e2.first_name || ' ' || e2.last_name as manager_name,
        e2.position as manager_position
    FROM employees e1
    JOIN departments d ON e1.department_id = d.department_id
    LEFT JOIN employees e2 ON e1.manager_id = e2.employee_id
    ORDER BY d.department_name, e1.last_name
    """
    
    df5 = pd.read_sql_query(query5, conn)
    print(df5.to_string(index=False))
    print("\n" + "="*80 + "\n")
    
    # Query 6: Project budget vs allocated hours
    print("6. Project budget vs allocated hours:")
    query6 = """
    SELECT 
        p.project_name,
        p.budget,
        SUM(ep.hours_allocated) as total_hours_allocated,
        ROUND(p.budget / SUM(ep.hours_allocated), 2) as budget_per_hour
    FROM projects p
    JOIN employee_projects ep ON p.project_id = ep.project_id
    GROUP BY p.project_id, p.project_name, p.budget
    HAVING total_hours_allocated > 0
    ORDER BY budget_per_hour DESC
    """
    
    df6 = pd.read_sql_query(query6, conn)
    print(df6.to_string(index=False))
    print("\n" + "="*80 + "\n")
    
    # Query 7: Employee workload (projects assigned)
    print("7. Employee workload (projects assigned):")
    query7 = """
    SELECT 
        e.first_name || ' ' || e.last_name as employee_name,
        d.department_name,
        COUNT(ep.project_id) as projects_assigned,
        SUM(ep.hours_allocated) as total_hours_allocated,
        GROUP_CONCAT(p.project_name, ', ') as project_names
    FROM employees e
    JOIN departments d ON e.department_id = d.department_id
    LEFT JOIN employee_projects ep ON e.employee_id = ep.employee_id
    LEFT JOIN projects p ON ep.project_id = p.project_id
    GROUP BY e.employee_id, e.first_name, e.last_name, d.department_name
    ORDER BY projects_assigned DESC, total_hours_allocated DESC
    """
    
    df7 = pd.read_sql_query(query7, conn)
    print(df7.to_string(index=False))
    print("\n" + "="*80 + "\n")
    
    conn.close()

def get_database_schema():
    """Print the database schema for reference"""
    conn = sqlite3.connect("employee_management.db")
    cursor = conn.cursor()
    
    print("=== DATABASE SCHEMA ===\n")
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        print(f"Table: {table_name}")
        print("-" * 50)
        
        # Get table schema
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        for col in columns:
            col_id, col_name, col_type, not_null, default_val, pk = col
            pk_str = " PRIMARY KEY" if pk else ""
            not_null_str = " NOT NULL" if not_null else ""
            default_str = f" DEFAULT {default_val}" if default_val else ""
            print(f"  {col_name} {col_type}{not_null_str}{default_str}{pk_str}")
        
        print()
    
    conn.close()

if __name__ == "__main__":
    # Print database schema
    get_database_schema()
    
    # Run sample queries
    test_database_queries()
    
    print("\n=== SAMPLE QUERIES FOR SQL AGENT TESTING ===")
    print("""
Here are some example queries you can test with your SQL agent:

1. "Show me all employees in the Engineering department"
2. "What is the average salary by department?"
3. "List all active projects with their budgets"
4. "Who are the managers and how many employees do they manage?"
5. "Show me employee attendance for the last week"
6. "Which employees are working on multiple projects?"
7. "What is the total budget allocated to each department?"
8. "Show me projects that are over budget"
9. "List employees who have been with the company for more than 2 years"
10. "What is the attendance rate by department?"
    """) 