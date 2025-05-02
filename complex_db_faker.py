"""
Generate functional mock sales database, which is required to create MCP 
"""

import sqlite3
from faker import Faker
import random

fake = Faker()

conn = sqlite3.connect('sales.db')
cursor = conn.cursor()

cursor.execute('PRAGMA foreign_keys = ON;')

cursor.execute('''CREATE TABLE IF NOT EXISTS Customers (
                    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT,
                    last_name TEXT,
                    email TEXT,
                    phone TEXT,
                    address TEXT)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Products (
                    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_name TEXT,
                    description TEXT,
                    price REAL,
                    stock_quantity INTEGER)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Orders (
                    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_id INTEGER,
                    order_date TEXT,
                    total_amount REAL,
                    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id))''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Order_Items (
                    order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER,
                    unit_price REAL,
                    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
                    FOREIGN KEY (product_id) REFERENCES Products(product_id))''')

customers = [(fake.first_name(), fake.last_name(), fake.email(), fake.phone_number(), fake.address()) for _ in range(100)]
cursor.executemany('INSERT INTO Customers (first_name, last_name, email, phone, address) VALUES (?, ?, ?, ?, ?)', customers)

customer_ids = [row[0] for row in cursor.execute('SELECT customer_id FROM Customers').fetchall()]

products = [(fake.word(), fake.sentence(), round(random.uniform(10.0, 100.0), 2), random.randint(50, 200)) for _ in range(50)]
cursor.executemany('INSERT INTO Products (product_name, description, price, stock_quantity) VALUES (?, ?, ?, ?)', products)

product_ids = [row[0] for row in cursor.execute('SELECT product_id FROM Products').fetchall()]

for _ in range(200):
    customer_id = random.choice(customer_ids)
    order_date = fake.date_between(start_date='-1y', end_date='today')
    num_items = random.randint(1, 5)  # Each order has 1 to 5 items
    total_amount = 0.0
    order_items_for_this_order = []
    
    for _ in range(num_items):
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 10)
        cursor.execute('SELECT price FROM Products WHERE product_id = ?', (product_id,))
        unit_price = cursor.fetchone()[0]
        subtotal = quantity * unit_price
        total_amount += subtotal
        order_items_for_this_order.append((product_id, quantity, unit_price))
    
    cursor.execute('INSERT INTO Orders (customer_id, order_date, total_amount) VALUES (?, ?, ?)', (customer_id, str(order_date), total_amount))
    order_id = cursor.lastrowid  # Get the auto-generated order_id
    
    for product_id, quantity, unit_price in order_items_for_this_order:
        cursor.execute('INSERT INTO Order_Items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)', (order_id, product_id, quantity, unit_price))


conn.commit()

cursor.execute('SELECT COUNT(*) FROM Customers')
print('Customers:', cursor.fetchone()[0])
cursor.execute('SELECT COUNT(*) FROM Products')
print('Products:', cursor.fetchone()[0])
cursor.execute('SELECT COUNT(*) FROM Orders')
print('Orders:', cursor.fetchone()[0])
cursor.execute('SELECT COUNT(*) FROM Order_Items')
print('Order Items:', cursor.fetchone()[0])


conn.close()