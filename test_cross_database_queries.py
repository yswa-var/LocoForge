#!/usr/bin/env python3
"""
Test script to demonstrate cross-database queries between 
grocery warehouse (NoSQL) and employee management (SQL) systems.
"""

import pymongo
from pymongo import MongoClient
import sqlite3
from datetime import datetime, timedelta

def connect_to_databases():
    """Connect to both MongoDB and SQLite databases."""
    # MongoDB connection
    mongo_client = MongoClient("mongodb://localhost:27017/")
    grocery_db = mongo_client["grocery_warehouse"]
    
    # SQLite connection
    sqlite_conn = sqlite3.connect("employee_management.db")
    
    return grocery_db, sqlite_conn

def test_cross_database_queries():
    """Test various cross-database queries."""
    grocery_db, sqlite_conn = connect_to_databases()
    mongo_client = grocery_db.client  # Get the client reference
    
    print("=" * 60)
    print("CROSS-DATABASE QUERY TESTS")
    print("=" * 60)
    
    # Test 1: Find employees who ordered products that are low in stock
    print("\n1. EMPLOYEES WHO ORDERED LOW STOCK PRODUCTS")
    print("-" * 40)
    
    # Get low stock products (current_stock < reorder_point)
    low_stock_products = list(grocery_db.inventory.find({
        "$expr": {
            "$lt": ["$stock_levels.current_stock", "$stock_levels.reorder_point"]
        }
    }))
    
    low_stock_product_ids = [item["product_id"] for item in low_stock_products]
    
    # Find orders containing low stock products
    orders_with_low_stock = list(grocery_db.orders.find({
        "items.product_id": {"$in": low_stock_product_ids}
    }))
    
    print(f"Low stock products: {low_stock_product_ids}")
    print(f"Orders with low stock products: {len(orders_with_low_stock)}")
    
    for order in orders_with_low_stock:
        employee = order.get("employee_info", {})
        print(f"  - {employee.get('employee_name', 'Unknown')} ({employee.get('department', 'Unknown')}) "
              f"ordered products that are low in stock")
    
    # Test 2: Show departments that ordered organic products
    print("\n2. DEPARTMENTS THAT ORDERED ORGANIC PRODUCTS")
    print("-" * 40)
    
    # Get organic products
    organic_products = list(grocery_db.products.find({
        "specifications.organic_certified": True
    }))
    
    organic_product_ids = [prod["product_id"] for prod in organic_products]
    
    # Find orders with organic products
    orders_with_organic = list(grocery_db.orders.find({
        "items.product_id": {"$in": organic_product_ids}
    }))
    
    departments_ordering_organic = {}
    for order in orders_with_organic:
        dept = order.get("employee_info", {}).get("department", "Unknown")
        if dept not in departments_ordering_organic:
            departments_ordering_organic[dept] = 0
        departments_ordering_organic[dept] += 1
    
    print(f"Organic products: {organic_product_ids}")
    print("Departments ordering organic products:")
    for dept, count in departments_ordering_organic.items():
        print(f"  - {dept}: {count} orders")
    
    # Test 3: Find project managers who placed large orders
    print("\n3. PROJECT MANAGERS WHO PLACED LARGE ORDERS")
    print("-" * 40)
    
    # Get SQL data for project managers
    cursor = sqlite_conn.cursor()
    cursor.execute("""
        SELECT e.employee_id, e.first_name, e.last_name, e.position, d.department_name
        FROM employees e
        JOIN departments d ON e.department_id = d.department_id
        WHERE e.position LIKE '%Manager%' OR e.position LIKE '%Director%'
    """)
    managers = cursor.fetchall()
    
    # Find large orders (total_amount > 500)
    large_orders = list(grocery_db.orders.find({
        "pricing.total_amount": {"$gt": 500}
    }))
    
    print("Project managers who placed large orders (>$500):")
    for order in large_orders:
        employee_id = order.get("employee_info", {}).get("employee_id")
        if employee_id:
            # Check if this employee is a manager
            for manager in managers:
                if manager[0] == employee_id:
                    print(f"  - {manager[1]} {manager[2]} ({manager[3]} - {manager[4]}) "
                          f"placed order ${order['pricing']['total_amount']:.2f}")
                    break
    
    # Test 4: Compare ordering patterns between departments
    print("\n4. ORDERING PATTERNS BY DEPARTMENT")
    print("-" * 40)
    
    dept_stats = {}
    for order in grocery_db.orders.find():
        dept = order.get("employee_info", {}).get("department", "Unknown")
        amount = order.get("pricing", {}).get("total_amount", 0)
        
        if dept not in dept_stats:
            dept_stats[dept] = {"count": 0, "total_amount": 0, "avg_amount": 0}
        
        dept_stats[dept]["count"] += 1
        dept_stats[dept]["total_amount"] += amount
    
    # Calculate averages
    for dept in dept_stats:
        if dept_stats[dept]["count"] > 0:
            dept_stats[dept]["avg_amount"] = dept_stats[dept]["total_amount"] / dept_stats[dept]["count"]
    
    print("Department ordering statistics:")
    for dept, stats in dept_stats.items():
        print(f"  - {dept}: {stats['count']} orders, "
              f"Total: ${stats['total_amount']:.2f}, "
              f"Avg: ${stats['avg_amount']:.2f}")
    
    # Test 5: Analyze employee productivity vs order patterns
    print("\n5. EMPLOYEE PRODUCTIVITY VS ORDER PATTERNS")
    print("-" * 40)
    
    # Get employee attendance data from SQL
    cursor.execute("""
        SELECT e.employee_id, e.first_name, e.last_name, e.salary,
               COUNT(CASE WHEN a.status = 'present' THEN 1 END) as days_present,
               COUNT(a.attendance_id) as total_days,
               AVG(a.hours_worked) as avg_hours
        FROM employees e
        LEFT JOIN attendance a ON e.employee_id = a.employee_id
        WHERE a.date >= date('now', '-30 days')
        GROUP BY e.employee_id, e.first_name, e.last_name, e.salary
        HAVING total_days > 0
    """)
    employee_attendance = cursor.fetchall()
    
    # Get order data for these employees
    employee_orders = {}
    for order in grocery_db.orders.find():
        employee_id = order.get("employee_info", {}).get("employee_id")
        if employee_id:
            if employee_id not in employee_orders:
                employee_orders[employee_id] = {"count": 0, "total_amount": 0}
            employee_orders[employee_id]["count"] += 1
            employee_orders[employee_id]["total_amount"] += order.get("pricing", {}).get("total_amount", 0)
    
    print("Employee productivity analysis (last 30 days):")
    for emp in employee_attendance:
        emp_id, first_name, last_name, salary, days_present, total_days, avg_hours = emp
        attendance_rate = (days_present / total_days * 100) if total_days > 0 else 0
        
        order_info = employee_orders.get(emp_id, {"count": 0, "total_amount": 0})
        
        print(f"  - {first_name} {last_name}:")
        print(f"    Attendance: {attendance_rate:.1f}% ({days_present}/{total_days} days)")
        print(f"    Avg hours: {avg_hours:.1f} hours/day")
        print(f"    Orders: {order_info['count']}, Total: ${order_info['total_amount']:.2f}")
        print(f"    Salary: ${salary:,.0f}")
        print()
    
    # Close connections
    mongo_client.close()
    sqlite_conn.close()

def test_complex_queries():
    """Test more complex cross-database queries."""
    grocery_db, sqlite_conn = connect_to_databases()
    
    print("\n" + "=" * 60)
    print("COMPLEX CROSS-DATABASE QUERIES")
    print("=" * 60)
    
    # Complex Query 1: Find high-performing employees (high salary + many orders)
    print("\n1. HIGH-PERFORMING EMPLOYEES ANALYSIS")
    print("-" * 40)
    
    cursor = sqlite_conn.cursor()
    cursor.execute("""
        SELECT e.employee_id, e.first_name, e.last_name, e.salary, d.department_name
        FROM employees e
        JOIN departments d ON e.department_id = d.department_id
        WHERE e.salary > 80000
        ORDER BY e.salary DESC
    """)
    high_salary_employees = cursor.fetchall()
    
    print("High-salary employees (>$80k) and their order performance:")
    for emp in high_salary_employees:
        emp_id, first_name, last_name, salary, dept = emp
        
        # Get their orders from MongoDB
        orders = list(grocery_db.orders.find({
            "employee_info.employee_id": emp_id
        }))
        
        total_order_amount = sum(order.get("pricing", {}).get("total_amount", 0) for order in orders)
        
        print(f"  - {first_name} {last_name} ({dept}):")
        print(f"    Salary: ${salary:,.0f}")
        print(f"    Orders: {len(orders)}, Total: ${total_order_amount:.2f}")
    
    # Complex Query 2: Department budget vs order volume correlation
    print("\n2. DEPARTMENT BUDGET VS ORDER VOLUME")
    print("-" * 40)
    
    cursor.execute("""
        SELECT d.department_name, d.budget, COUNT(e.employee_id) as employee_count
        FROM departments d
        LEFT JOIN employees e ON d.department_id = e.department_id
        GROUP BY d.department_id, d.department_name, d.budget
        ORDER BY d.budget DESC
    """)
    dept_budgets = cursor.fetchall()
    
    print("Department budgets and order volumes:")
    for dept_name, budget, emp_count in dept_budgets:
        # Get orders for this department
        orders = list(grocery_db.orders.find({
            "employee_info.department": dept_name
        }))
        
        total_order_amount = sum(order.get("pricing", {}).get("total_amount", 0) for order in orders)
        
        print(f"  - {dept_name}:")
        print(f"    Budget: ${budget:,.0f}, Employees: {emp_count}")
        print(f"    Orders: {len(orders)}, Total: ${total_order_amount:.2f}")
    
    # Close connections
    mongo_client = grocery_db.client
    mongo_client.close()
    sqlite_conn.close()

if __name__ == "__main__":
    try:
        test_cross_database_queries()
        test_complex_queries()
        print("\n" + "=" * 60)
        print("ALL CROSS-DATABASE TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 60)
    except Exception as e:
        print(f"Error running tests: {e}")
        import traceback
        traceback.print_exc() 