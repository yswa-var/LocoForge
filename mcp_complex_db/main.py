from fastmcp import FastMCP
import sqlite3
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime, timedelta

# Initialize FastMCP server
mcp = FastMCP(
    name="SalesAnalytics",
    instructions="This server provides access to sales data and analytics tools. You can query sales data, get customer insights, and analyze product performance."
)

# Database connection helper
def get_db_connection():
    return sqlite3.connect('sales.db')

@mcp.tool()
def get_sales_summary() -> Dict[str, Any]:
    """Get a summary of sales data including total revenue, number of orders, and average order value."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get total revenue
    cursor.execute('SELECT SUM(total_amount) FROM Orders')
    total_revenue = cursor.fetchone()[0]
    
    # Get number of orders
    cursor.execute('SELECT COUNT(*) FROM Orders')
    num_orders = cursor.fetchone()[0]
    
    # Get average order value
    cursor.execute('SELECT AVG(total_amount) FROM Orders')
    avg_order_value = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        "total_revenue": round(total_revenue, 2),
        "number_of_orders": num_orders,
        "average_order_value": round(avg_order_value, 2)
    }

@mcp.tool()
def get_top_products(limit: int = 5) -> List[Dict[str, Any]]:
    """Get the top selling products by revenue."""
    conn = get_db_connection()
    query = """
    SELECT p.product_name, SUM(oi.quantity * oi.unit_price) as total_revenue
    FROM Products p
    JOIN Order_Items oi ON p.product_id = oi.product_id
    GROUP BY p.product_id
    ORDER BY total_revenue DESC
    LIMIT ?
    """
    df = pd.read_sql_query(query, conn, params=(limit,))
    conn.close()
    return df.to_dict('records')

@mcp.tool()
def get_customer_insights(customer_id: int) -> Dict[str, Any]:
    """Get detailed insights about a specific customer's purchasing behavior."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get customer details
    cursor.execute('SELECT first_name, last_name, email FROM Customers WHERE customer_id = ?', (customer_id,))
    customer = cursor.fetchone()
    
    if not customer:
        return {"error": "Customer not found"}
    
    # Get customer's order history
    cursor.execute('''
        SELECT o.order_date, o.total_amount
        FROM Orders o
        WHERE o.customer_id = ?
        ORDER BY o.order_date DESC
    ''', (customer_id,))
    orders = cursor.fetchall()
    
    # Get customer's favorite products
    cursor.execute('''
        SELECT p.product_name, COUNT(*) as purchase_count
        FROM Order_Items oi
        JOIN Products p ON oi.product_id = p.product_id
        JOIN Orders o ON oi.order_id = o.order_id
        WHERE o.customer_id = ?
        GROUP BY p.product_id
        ORDER BY purchase_count DESC
        LIMIT 3
    ''', (customer_id,))
    favorite_products = cursor.fetchall()
    
    conn.close()
    
    return {
        "customer_name": f"{customer[0]} {customer[1]}",
        "email": customer[2],
        "total_orders": len(orders),
        "total_spent": sum(order[1] for order in orders),
        "favorite_products": [{"product": p[0], "purchases": p[1]} for p in favorite_products]
    }

@mcp.tool()
def get_sales_trends(days: int = 30) -> List[Dict[str, Any]]:
    """Get daily sales trends for the specified number of days."""
    conn = get_db_connection()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    query = """
    SELECT date(order_date) as date, SUM(total_amount) as daily_revenue
    FROM Orders
    WHERE order_date >= ? AND order_date <= ?
    GROUP BY date(order_date)
    ORDER BY date
    """
    
    df = pd.read_sql_query(query, conn, params=(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    conn.close()
    return df.to_dict('records')

if __name__ == "__main__":
    mcp.run()
