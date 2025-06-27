import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import random
from typing import Dict, List, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GroceryWarehouseDB:
    def __init__(self, connection_string: str = "mongodb://localhost:27017/"):
        """Initialize MongoDB connection for grocery warehouse database."""
        self.client = MongoClient(connection_string)
        self.db_name = "grocery_warehouse"
        self.db = self.client[self.db_name]
        
    def check_database_exists(self) -> bool:
        """Check if the grocery warehouse database exists."""
        try:
            database_names = self.client.list_database_names()
            return self.db_name in database_names
        except Exception as e:
            logger.error(f"Error checking database existence: {e}")
            return False
    
    def create_database(self) -> bool:
        """Create the grocery warehouse database with all collections and sample data."""
        try:
            if self.check_database_exists():
                logger.info(f"Database '{self.db_name}' already exists. Dropping and recreating...")
                self.client.drop_database(self.db_name)
                self.db = self.client[self.db_name]
            
            # Create collections
            self._create_employees_collection()
            self._create_products_collection()
            self._create_inventory_collection()
            self._create_orders_collection()
            
            # Create indexes for better performance
            self._create_indexes()
            
            logger.info(f"Database '{self.db_name}' created successfully with all collections and sample data.")
            return True
            
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return False
    
    def _create_employees_collection(self):
        """Create employees collection that mirrors the SQL database structure."""
        employees_data = [
            # Engineering Department (8 employees)
            {
                "employee_id": 1,
                "first_name": "John",
                "last_name": "Smith",
                "email": "john.smith@company.com",
                "phone": "555-0101",
                "hire_date": datetime(2020, 1, 15),
                "salary": 95000.0,
                "department_id": 1,
                "department_name": "Engineering",
                "manager_id": None,
                "position": "Senior Software Engineer",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 2,
                "first_name": "Sarah",
                "last_name": "Johnson",
                "email": "sarah.johnson@company.com",
                "phone": "555-0102",
                "hire_date": datetime(2021, 3, 20),
                "salary": 85000.0,
                "department_id": 1,
                "department_name": "Engineering",
                "manager_id": 1,
                "position": "Software Engineer",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 3,
                "first_name": "Mike",
                "last_name": "Davis",
                "email": "mike.davis@company.com",
                "phone": "555-0103",
                "hire_date": datetime(2022, 6, 10),
                "salary": 75000.0,
                "department_id": 1,
                "department_name": "Engineering",
                "manager_id": 1,
                "position": "Junior Developer",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 4,
                "first_name": "Emily",
                "last_name": "Wilson",
                "email": "emily.wilson@company.com",
                "phone": "555-0104",
                "hire_date": datetime(2021, 8, 15),
                "salary": 80000.0,
                "department_id": 1,
                "department_name": "Engineering",
                "manager_id": 1,
                "position": "QA Engineer",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 5,
                "first_name": "Alex",
                "last_name": "Chen",
                "email": "alex.chen@company.com",
                "phone": "555-0105",
                "hire_date": datetime(2023, 1, 10),
                "salary": 70000.0,
                "department_id": 1,
                "department_name": "Engineering",
                "manager_id": 1,
                "position": "Frontend Developer",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            # Marketing Department (6 employees)
            {
                "employee_id": 9,
                "first_name": "Robert",
                "last_name": "Taylor",
                "email": "robert.taylor@company.com",
                "phone": "555-0201",
                "hire_date": datetime(2019, 11, 5),
                "salary": 82000.0,
                "department_id": 2,
                "department_name": "Marketing",
                "manager_id": None,
                "position": "Marketing Manager",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 10,
                "first_name": "Amanda",
                "last_name": "Anderson",
                "email": "amanda.anderson@company.com",
                "phone": "555-0202",
                "hire_date": datetime(2022, 1, 10),
                "salary": 65000.0,
                "department_id": 2,
                "department_name": "Marketing",
                "manager_id": 9,
                "position": "Marketing Specialist",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 11,
                "first_name": "Chris",
                "last_name": "Martinez",
                "email": "chris.martinez@company.com",
                "phone": "555-0203",
                "hire_date": datetime(2023, 2, 28),
                "salary": 58000.0,
                "department_id": 2,
                "department_name": "Marketing",
                "manager_id": 9,
                "position": "Content Creator",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            # Sales Department (7 employees)
            {
                "employee_id": 15,
                "first_name": "Patricia",
                "last_name": "Robinson",
                "email": "patricia.robinson@company.com",
                "phone": "555-0301",
                "hire_date": datetime(2018, 5, 12),
                "salary": 90000.0,
                "department_id": 3,
                "department_name": "Sales",
                "manager_id": None,
                "position": "Sales Director",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 16,
                "first_name": "Kevin",
                "last_name": "Clark",
                "email": "kevin.clark@company.com",
                "phone": "555-0302",
                "hire_date": datetime(2021, 9, 20),
                "salary": 70000.0,
                "department_id": 3,
                "department_name": "Sales",
                "manager_id": 15,
                "position": "Sales Representative",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 17,
                "first_name": "Maria",
                "last_name": "Rodriguez",
                "email": "maria.rodriguez@company.com",
                "phone": "555-0303",
                "hire_date": datetime(2022, 12, 1),
                "salary": 68000.0,
                "department_id": 3,
                "department_name": "Sales",
                "manager_id": 15,
                "position": "Sales Representative",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            # HR Department (5 employees)
            {
                "employee_id": 22,
                "first_name": "Elizabeth",
                "last_name": "Allen",
                "email": "elizabeth.allen@company.com",
                "phone": "555-0401",
                "hire_date": datetime(2020, 2, 14),
                "salary": 78000.0,
                "department_id": 4,
                "department_name": "Human Resources",
                "manager_id": None,
                "position": "HR Manager",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 23,
                "first_name": "Christopher",
                "last_name": "King",
                "email": "christopher.king@company.com",
                "phone": "555-0402",
                "hire_date": datetime(2023, 4, 15),
                "salary": 62000.0,
                "department_id": 4,
                "department_name": "Human Resources",
                "manager_id": 22,
                "position": "HR Specialist",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            # Finance Department (6 employees)
            {
                "employee_id": 27,
                "first_name": "Andrew",
                "last_name": "Scott",
                "email": "andrew.scott@company.com",
                "phone": "555-0501",
                "hire_date": datetime(2019, 7, 22),
                "salary": 85000.0,
                "department_id": 5,
                "department_name": "Finance",
                "manager_id": None,
                "position": "Finance Manager",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 28,
                "first_name": "Samantha",
                "last_name": "Green",
                "email": "samantha.green@company.com",
                "phone": "555-0502",
                "hire_date": datetime(2022, 8, 30),
                "salary": 72000.0,
                "department_id": 5,
                "department_name": "Finance",
                "manager_id": 27,
                "position": "Accountant",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            # Operations Department (5 employees)
            {
                "employee_id": 33,
                "first_name": "Joshua",
                "last_name": "Mitchell",
                "email": "joshua.mitchell@company.com",
                "phone": "555-0601",
                "hire_date": datetime(2021, 1, 10),
                "salary": 88000.0,
                "department_id": 6,
                "department_name": "Operations",
                "manager_id": None,
                "position": "Operations Manager",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 34,
                "first_name": "Ashley",
                "last_name": "Perez",
                "email": "ashley.perez@company.com",
                "phone": "555-0602",
                "hire_date": datetime(2023, 3, 15),
                "salary": 64000.0,
                "department_id": 6,
                "department_name": "Operations",
                "manager_id": 33,
                "position": "Operations Coordinator",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            # IT Support Department (4 employees)
            {
                "employee_id": 38,
                "first_name": "Lauren",
                "last_name": "Campbell",
                "email": "lauren.campbell@company.com",
                "phone": "555-0701",
                "hire_date": datetime(2020, 8, 15),
                "salary": 75000.0,
                "department_id": 7,
                "department_name": "IT Support",
                "manager_id": None,
                "position": "IT Support Manager",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 39,
                "first_name": "Tyler",
                "last_name": "Parker",
                "email": "tyler.parker@company.com",
                "phone": "555-0702",
                "hire_date": datetime(2022, 10, 20),
                "salary": 58000.0,
                "department_id": 7,
                "department_name": "IT Support",
                "manager_id": 38,
                "position": "IT Support Specialist",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            # Research & Development Department (6 employees)
            {
                "employee_id": 42,
                "first_name": "Rebecca",
                "last_name": "Collins",
                "email": "rebecca.collins@company.com",
                "phone": "555-0801",
                "hire_date": datetime(2018, 12, 1),
                "salary": 120000.0,
                "department_id": 8,
                "department_name": "Research & Development",
                "manager_id": None,
                "position": "R&D Director",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 43,
                "first_name": "Nathan",
                "last_name": "Stewart",
                "email": "nathan.stewart@company.com",
                "phone": "555-0802",
                "hire_date": datetime(2021, 5, 15),
                "salary": 95000.0,
                "department_id": 8,
                "department_name": "Research & Development",
                "manager_id": 42,
                "position": "Senior Research Scientist",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            # Customer Service Department (4 employees)
            {
                "employee_id": 48,
                "first_name": "Alyssa",
                "last_name": "Cook",
                "email": "alyssa.cook@company.com",
                "phone": "555-0901",
                "hire_date": datetime(2021, 3, 10),
                "salary": 65000.0,
                "department_id": 9,
                "department_name": "Customer Service",
                "manager_id": None,
                "position": "Customer Service Manager",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 49,
                "first_name": "Ethan",
                "last_name": "Morgan",
                "email": "ethan.morgan@company.com",
                "phone": "555-0902",
                "hire_date": datetime(2022, 11, 15),
                "salary": 52000.0,
                "department_id": 9,
                "department_name": "Customer Service",
                "manager_id": 48,
                "position": "Customer Service Representative",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            # Legal Department (3 employees)
            {
                "employee_id": 52,
                "first_name": "Chloe",
                "last_name": "Bailey",
                "email": "chloe.bailey@company.com",
                "phone": "555-1001",
                "hire_date": datetime(2019, 6, 20),
                "salary": 110000.0,
                "department_id": 10,
                "department_name": "Legal",
                "manager_id": None,
                "position": "Legal Counsel",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "employee_id": 53,
                "first_name": "Mason",
                "last_name": "Rivera",
                "email": "mason.rivera@company.com",
                "phone": "555-1002",
                "hire_date": datetime(2022, 3, 15),
                "salary": 85000.0,
                "department_id": 10,
                "department_name": "Legal",
                "manager_id": 52,
                "position": "Legal Assistant",
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            }
        ]
        
        self.db.employees.insert_many(employees_data)
        logger.info(f"Created employees collection with {len(employees_data)} employees")
    
    def _create_products_collection(self):
        """Create products collection with complex product data."""
        products_data = [
            {
                "product_id": "PROD001",
                "name": "Organic Bananas",
                "category": "Fruits",
                "subcategory": "Tropical Fruits",
                "brand": "FreshHarvest",
                "description": "Premium organic bananas from sustainable farms",
                "specifications": {
                    "weight_per_unit": "150g",
                    "origin": "Ecuador",
                    "organic_certified": True,
                    "allergens": [],
                    "nutritional_info": {
                        "calories_per_100g": 89,
                        "protein": "1.1g",
                        "carbs": "23g",
                        "fiber": "2.6g"
                    }
                },
                "pricing": {
                    "cost_price": 0.45,
                    "selling_price": 0.89,
                    "currency": "USD",
                    "bulk_discounts": [
                        {"quantity": 10, "discount_percent": 5},
                        {"quantity": 50, "discount_percent": 10}
                    ]
                },
                "supplier_info": {
                    "supplier_id": "SUPP001",
                    "supplier_name": "Tropical Fruits Co.",
                    "contact": {
                        "email": "orders@tropicalfruits.com",
                        "phone": "+1-555-0123"
                    },
                    "lead_time_days": 3
                },
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "active": True
            },
            {
                "product_id": "PROD002",
                "name": "Whole Grain Bread",
                "category": "Bakery",
                "subcategory": "Bread",
                "brand": "ArtisanBake",
                "description": "Fresh whole grain bread with seeds and nuts",
                "specifications": {
                    "weight_per_unit": "500g",
                    "ingredients": ["whole wheat flour", "seeds", "nuts", "yeast", "salt"],
                    "organic_certified": True,
                    "allergens": ["gluten", "nuts"],
                    "nutritional_info": {
                        "calories_per_100g": 265,
                        "protein": "9g",
                        "carbs": "45g",
                        "fiber": "8g"
                    },
                    "shelf_life_days": 7
                },
                "pricing": {
                    "cost_price": 1.20,
                    "selling_price": 2.49,
                    "currency": "USD",
                    "bulk_discounts": [
                        {"quantity": 5, "discount_percent": 8},
                        {"quantity": 20, "discount_percent": 15}
                    ]
                },
                "supplier_info": {
                    "supplier_id": "SUPP002",
                    "supplier_name": "Artisan Bakery Ltd.",
                    "contact": {
                        "email": "wholesale@artisanbake.com",
                        "phone": "+1-555-0456"
                    },
                    "lead_time_days": 1
                },
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "active": True
            },
            {
                "product_id": "PROD003",
                "name": "Greek Yogurt",
                "category": "Dairy",
                "subcategory": "Yogurt",
                "brand": "CreamyDairy",
                "description": "High-protein Greek yogurt with live cultures",
                "specifications": {
                    "weight_per_unit": "170g",
                    "fat_content": "2%",
                    "protein_content": "15g",
                    "organic_certified": False,
                    "allergens": ["milk"],
                    "nutritional_info": {
                        "calories_per_100g": 130,
                        "protein": "15g",
                        "carbs": "8g",
                        "fat": "2g"
                    },
                    "shelf_life_days": 21
                },
                "pricing": {
                    "cost_price": 0.85,
                    "selling_price": 1.79,
                    "currency": "USD",
                    "bulk_discounts": [
                        {"quantity": 12, "discount_percent": 6},
                        {"quantity": 48, "discount_percent": 12}
                    ]
                },
                "supplier_info": {
                    "supplier_id": "SUPP003",
                    "supplier_name": "Creamy Dairy Farms",
                    "contact": {
                        "email": "wholesale@creamydairy.com",
                        "phone": "+1-555-0789"
                    },
                    "lead_time_days": 2
                },
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "active": True
            },
            {
                "product_id": "PROD004",
                "name": "Organic Spinach",
                "category": "Vegetables",
                "subcategory": "Leafy Greens",
                "brand": "GreenHarvest",
                "description": "Fresh organic spinach leaves, pre-washed",
                "specifications": {
                    "weight_per_unit": "200g",
                    "origin": "Local Farms",
                    "organic_certified": True,
                    "allergens": [],
                    "nutritional_info": {
                        "calories_per_100g": 23,
                        "protein": "2.9g",
                        "carbs": "3.6g",
                        "fiber": "2.2g"
                    },
                    "shelf_life_days": 7
                },
                "pricing": {
                    "cost_price": 0.60,
                    "selling_price": 1.29,
                    "currency": "USD",
                    "bulk_discounts": [
                        {"quantity": 10, "discount_percent": 7},
                        {"quantity": 25, "discount_percent": 12}
                    ]
                },
                "supplier_info": {
                    "supplier_id": "SUPP004",
                    "supplier_name": "Local Organic Farms",
                    "contact": {
                        "email": "orders@localorganic.com",
                        "phone": "+1-555-0321"
                    },
                    "lead_time_days": 1
                },
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "active": True
            },
            {
                "product_id": "PROD005",
                "name": "Extra Virgin Olive Oil",
                "category": "Pantry",
                "subcategory": "Oils",
                "brand": "MediterraneanGold",
                "description": "Premium extra virgin olive oil from Italian groves",
                "specifications": {
                    "volume_per_unit": "500ml",
                    "origin": "Italy",
                    "organic_certified": True,
                    "allergens": [],
                    "nutritional_info": {
                        "calories_per_100ml": 884,
                        "fat": "100g",
                        "saturated_fat": "14g"
                    },
                    "shelf_life_days": 730
                },
                "pricing": {
                    "cost_price": 3.50,
                    "selling_price": 7.99,
                    "currency": "USD",
                    "bulk_discounts": [
                        {"quantity": 6, "discount_percent": 8},
                        {"quantity": 24, "discount_percent": 15}
                    ]
                },
                "supplier_info": {
                    "supplier_id": "SUPP005",
                    "supplier_name": "Mediterranean Imports",
                    "contact": {
                        "email": "orders@medimports.com",
                        "phone": "+1-555-0654"
                    },
                    "lead_time_days": 14
                },
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "active": True
            }
        ]
        
        self.db.products.insert_many(products_data)
        logger.info(f"Created products collection with {len(products_data)} products")
    
    def _create_inventory_collection(self):
        """Create inventory collection with complex inventory tracking."""
        inventory_data = [
            {
                "inventory_id": "INV001",
                "product_id": "PROD001",
                "warehouse_location": {
                    "zone": "A",
                    "aisle": "1",
                    "shelf": "3",
                    "position": "left"
                },
                "stock_levels": {
                    "current_stock": 1250,
                    "minimum_stock": 200,
                    "maximum_stock": 2000,
                    "reorder_point": 300
                },
                "batch_info": [
                    {
                        "batch_id": "BATCH001-001",
                        "quantity": 800,
                        "expiry_date": datetime.now() + timedelta(days=7),
                        "supplier_batch": "TB-2024-001",
                        "received_date": datetime.now() - timedelta(days=2),
                        "quality_status": "excellent"
                    },
                    {
                        "batch_id": "BATCH001-002",
                        "quantity": 450,
                        "expiry_date": datetime.now() + timedelta(days=5),
                        "supplier_batch": "TB-2024-002",
                        "received_date": datetime.now() - timedelta(days=1),
                        "quality_status": "good"
                    }
                ],
                "movement_history": [
                    {
                        "date": datetime.now() - timedelta(days=1),
                        "type": "in",
                        "quantity": 450,
                        "reference": "PO-2024-001"
                    },
                    {
                        "date": datetime.now() - timedelta(hours=6),
                        "type": "out",
                        "quantity": 150,
                        "reference": "SO-2024-015"
                    }
                ],
                "last_updated": datetime.now()
            },
            {
                "inventory_id": "INV002",
                "product_id": "PROD002",
                "warehouse_location": {
                    "zone": "B",
                    "aisle": "2",
                    "shelf": "1",
                    "position": "center"
                },
                "stock_levels": {
                    "current_stock": 85,
                    "minimum_stock": 50,
                    "maximum_stock": 300,
                    "reorder_point": 75
                },
                "batch_info": [
                    {
                        "batch_id": "BATCH002-001",
                        "quantity": 85,
                        "expiry_date": datetime.now() + timedelta(days=6),
                        "supplier_batch": "AB-2024-001",
                        "received_date": datetime.now() - timedelta(hours=12),
                        "quality_status": "excellent"
                    }
                ],
                "movement_history": [
                    {
                        "date": datetime.now() - timedelta(hours=12),
                        "type": "in",
                        "quantity": 100,
                        "reference": "PO-2024-002"
                    },
                    {
                        "date": datetime.now() - timedelta(hours=2),
                        "type": "out",
                        "quantity": 15,
                        "reference": "SO-2024-016"
                    }
                ],
                "last_updated": datetime.now()
            },
            {
                "inventory_id": "INV003",
                "product_id": "PROD003",
                "warehouse_location": {
                    "zone": "C",
                    "aisle": "3",
                    "shelf": "2",
                    "position": "right"
                },
                "stock_levels": {
                    "current_stock": 420,
                    "minimum_stock": 100,
                    "maximum_stock": 800,
                    "reorder_point": 150
                },
                "batch_info": [
                    {
                        "batch_id": "BATCH003-001",
                        "quantity": 420,
                        "expiry_date": datetime.now() + timedelta(days=18),
                        "supplier_batch": "CD-2024-001",
                        "received_date": datetime.now() - timedelta(days=3),
                        "quality_status": "excellent"
                    }
                ],
                "movement_history": [
                    {
                        "date": datetime.now() - timedelta(days=3),
                        "type": "in",
                        "quantity": 500,
                        "reference": "PO-2024-003"
                    },
                    {
                        "date": datetime.now() - timedelta(hours=8),
                        "type": "out",
                        "quantity": 80,
                        "reference": "SO-2024-017"
                    }
                ],
                "last_updated": datetime.now()
            },
            {
                "inventory_id": "INV004",
                "product_id": "PROD004",
                "warehouse_location": {
                    "zone": "A",
                    "aisle": "1",
                    "shelf": "1",
                    "position": "center"
                },
                "stock_levels": {
                    "current_stock": 75,
                    "minimum_stock": 30,
                    "maximum_stock": 150,
                    "reorder_point": 50
                },
                "batch_info": [
                    {
                        "batch_id": "BATCH004-001",
                        "quantity": 75,
                        "expiry_date": datetime.now() + timedelta(days=6),
                        "supplier_batch": "LOF-2024-001",
                        "received_date": datetime.now() - timedelta(hours=6),
                        "quality_status": "excellent"
                    }
                ],
                "movement_history": [
                    {
                        "date": datetime.now() - timedelta(hours=6),
                        "type": "in",
                        "quantity": 100,
                        "reference": "PO-2024-004"
                    },
                    {
                        "date": datetime.now() - timedelta(hours=1),
                        "type": "out",
                        "quantity": 25,
                        "reference": "SO-2024-018"
                    }
                ],
                "last_updated": datetime.now()
            },
            {
                "inventory_id": "INV005",
                "product_id": "PROD005",
                "warehouse_location": {
                    "zone": "D",
                    "aisle": "4",
                    "shelf": "1",
                    "position": "left"
                },
                "stock_levels": {
                    "current_stock": 180,
                    "minimum_stock": 50,
                    "maximum_stock": 400,
                    "reorder_point": 80
                },
                "batch_info": [
                    {
                        "batch_id": "BATCH005-001",
                        "quantity": 180,
                        "expiry_date": datetime.now() + timedelta(days=700),
                        "supplier_batch": "MI-2024-001",
                        "received_date": datetime.now() - timedelta(days=5),
                        "quality_status": "excellent"
                    }
                ],
                "movement_history": [
                    {
                        "date": datetime.now() - timedelta(days=5),
                        "type": "in",
                        "quantity": 200,
                        "reference": "PO-2024-005"
                    },
                    {
                        "date": datetime.now() - timedelta(hours=4),
                        "type": "out",
                        "quantity": 20,
                        "reference": "SO-2024-019"
                    }
                ],
                "last_updated": datetime.now()
            }
        ]
        
        self.db.inventory.insert_many(inventory_data)
        logger.info(f"Created inventory collection with {len(inventory_data)} inventory records")
    
    def _create_orders_collection(self):
        """Create orders collection with complex order data including employee relationships."""
        orders_data = [
            {
                "order_id": "ORD001",
                "customer_info": {
                    "customer_id": "CUST001",
                    "name": "Fresh Market Chain",
                    "type": "retail_chain",
                    "contact": {
                        "email": "orders@freshmarket.com",
                        "phone": "+1-555-1000",
                        "address": {
                            "street": "123 Market St",
                            "city": "Downtown",
                            "state": "CA",
                            "zip": "90210"
                        }
                    },
                    "credit_limit": 50000,
                    "payment_terms": "net_30"
                },
                "employee_info": {
                    "employee_id": 15,  # Patricia Robinson - Sales Director
                    "employee_name": "Patricia Robinson",
                    "department": "Sales",
                    "position": "Sales Director",
                    "email": "patricia.robinson@company.com"
                },
                "order_details": {
                    "order_date": datetime.now() - timedelta(days=2),
                    "delivery_date": datetime.now() + timedelta(days=1),
                    "status": "processing",
                    "priority": "high",
                    "delivery_method": "express",
                    "special_instructions": "Keep refrigerated items separate"
                },
                "items": [
                    {
                        "product_id": "PROD001",
                        "quantity": 500,
                        "unit_price": 0.89,
                        "total_price": 445.00,
                        "discount_applied": 0.05,
                        "final_price": 422.75
                    },
                    {
                        "product_id": "PROD003",
                        "quantity": 200,
                        "unit_price": 1.79,
                        "total_price": 358.00,
                        "discount_applied": 0.06,
                        "final_price": 336.52
                    }
                ],
                "pricing": {
                    "subtotal": 803.00,
                    "tax_rate": 0.085,
                    "tax_amount": 68.26,
                    "shipping_cost": 25.00,
                    "total_amount": 896.26
                },
                "payment_info": {
                    "method": "credit_card",
                    "status": "pending",
                    "transaction_id": None
                },
                "created_at": datetime.now() - timedelta(days=2),
                "updated_at": datetime.now()
            },
            {
                "order_id": "ORD002",
                "customer_info": {
                    "customer_id": "CUST002",
                    "name": "Local Grocery Store",
                    "type": "independent",
                    "contact": {
                        "email": "orders@localgrocery.com",
                        "phone": "+1-555-2000",
                        "address": {
                            "street": "456 Main Ave",
                            "city": "Suburbia",
                            "state": "CA",
                            "zip": "90211"
                        }
                    },
                    "credit_limit": 15000,
                    "payment_terms": "net_15"
                },
                "employee_info": {
                    "employee_id": 16,  # Kevin Clark - Sales Representative
                    "employee_name": "Kevin Clark",
                    "department": "Sales",
                    "position": "Sales Representative",
                    "email": "kevin.clark@company.com"
                },
                "order_details": {
                    "order_date": datetime.now() - timedelta(days=1),
                    "delivery_date": datetime.now() + timedelta(days=2),
                    "status": "confirmed",
                    "priority": "normal",
                    "delivery_method": "standard",
                    "special_instructions": "Deliver before 2 PM"
                },
                "items": [
                    {
                        "product_id": "PROD002",
                        "quantity": 50,
                        "unit_price": 2.49,
                        "total_price": 124.50,
                        "discount_applied": 0.08,
                        "final_price": 114.54
                    },
                    {
                        "product_id": "PROD004",
                        "quantity": 30,
                        "unit_price": 1.29,
                        "total_price": 38.70,
                        "discount_applied": 0.07,
                        "final_price": 35.99
                    },
                    {
                        "product_id": "PROD005",
                        "quantity": 10,
                        "unit_price": 7.99,
                        "total_price": 79.90,
                        "discount_applied": 0.08,
                        "final_price": 73.51
                    }
                ],
                "pricing": {
                    "subtotal": 243.10,
                    "tax_rate": 0.085,
                    "tax_amount": 20.66,
                    "shipping_cost": 15.00,
                    "total_amount": 278.76
                },
                "payment_info": {
                    "method": "bank_transfer",
                    "status": "paid",
                    "transaction_id": "TXN-2024-001"
                },
                "created_at": datetime.now() - timedelta(days=1),
                "updated_at": datetime.now()
            },
            {
                "order_id": "ORD003",
                "customer_info": {
                    "customer_id": "CUST003",
                    "name": "Restaurant Supply Co.",
                    "type": "wholesale",
                    "contact": {
                        "email": "orders@restsupply.com",
                        "phone": "+1-555-3000",
                        "address": {
                            "street": "789 Business Blvd",
                            "city": "Industrial",
                            "state": "CA",
                            "zip": "90212"
                        }
                    },
                    "credit_limit": 100000,
                    "payment_terms": "net_45"
                },
                "employee_info": {
                    "employee_id": 17,  # Maria Rodriguez - Sales Representative
                    "employee_name": "Maria Rodriguez",
                    "department": "Sales",
                    "position": "Sales Representative",
                    "email": "maria.rodriguez@company.com"
                },
                "order_details": {
                    "order_date": datetime.now(),
                    "delivery_date": datetime.now() + timedelta(days=3),
                    "status": "pending",
                    "priority": "medium",
                    "delivery_method": "standard",
                    "special_instructions": "Palletized delivery required"
                },
                "items": [
                    {
                        "product_id": "PROD001",
                        "quantity": 1000,
                        "unit_price": 0.89,
                        "total_price": 890.00,
                        "discount_applied": 0.10,
                        "final_price": 801.00
                    },
                    {
                        "product_id": "PROD003",
                        "quantity": 500,
                        "unit_price": 1.79,
                        "total_price": 895.00,
                        "discount_applied": 0.12,
                        "final_price": 787.60
                    },
                    {
                        "product_id": "PROD005",
                        "quantity": 100,
                        "unit_price": 7.99,
                        "total_price": 799.00,
                        "discount_applied": 0.15,
                        "final_price": 679.15
                    }
                ],
                "pricing": {
                    "subtotal": 2584.00,
                    "tax_rate": 0.085,
                    "tax_amount": 219.64,
                    "shipping_cost": 50.00,
                    "total_amount": 2853.64
                },
                "payment_info": {
                    "method": "invoice",
                    "status": "pending",
                    "transaction_id": None
                },
                "created_at": datetime.now(),
                "updated_at": datetime.now()
            },
            {
                "order_id": "ORD004",
                "customer_info": {
                    "customer_id": "CUST004",
                    "name": "Tech Company Cafeteria",
                    "type": "corporate",
                    "contact": {
                        "email": "cafeteria@techcompany.com",
                        "phone": "+1-555-4000",
                        "address": {
                            "street": "321 Innovation Dr",
                            "city": "Tech Park",
                            "state": "CA",
                            "zip": "90213"
                        }
                    },
                    "credit_limit": 25000,
                    "payment_terms": "net_30"
                },
                "employee_info": {
                    "employee_id": 33,  # Joshua Mitchell - Operations Manager
                    "employee_name": "Joshua Mitchell",
                    "department": "Operations",
                    "position": "Operations Manager",
                    "email": "joshua.mitchell@company.com"
                },
                "order_details": {
                    "order_date": datetime.now() - timedelta(days=3),
                    "delivery_date": datetime.now() + timedelta(days=1),
                    "status": "processing",
                    "priority": "high",
                    "delivery_method": "express",
                    "special_instructions": "Organic products only"
                },
                "items": [
                    {
                        "product_id": "PROD001",  # Organic Bananas
                        "quantity": 200,
                        "unit_price": 0.89,
                        "total_price": 178.00,
                        "discount_applied": 0.05,
                        "final_price": 169.10
                    },
                    {
                        "product_id": "PROD002",  # Whole Grain Bread (organic)
                        "quantity": 100,
                        "unit_price": 2.49,
                        "total_price": 249.00,
                        "discount_applied": 0.08,
                        "final_price": 229.08
                    },
                    {
                        "product_id": "PROD004",  # Organic Spinach
                        "quantity": 50,
                        "unit_price": 1.29,
                        "total_price": 64.50,
                        "discount_applied": 0.07,
                        "final_price": 59.99
                    },
                    {
                        "product_id": "PROD005",  # Extra Virgin Olive Oil (organic)
                        "quantity": 25,
                        "unit_price": 7.99,
                        "total_price": 199.75,
                        "discount_applied": 0.08,
                        "final_price": 183.77
                    }
                ],
                "pricing": {
                    "subtotal": 691.25,
                    "tax_rate": 0.085,
                    "tax_amount": 58.76,
                    "shipping_cost": 20.00,
                    "total_amount": 770.01
                },
                "payment_info": {
                    "method": "credit_card",
                    "status": "paid",
                    "transaction_id": "TXN-2024-002"
                },
                "created_at": datetime.now() - timedelta(days=3),
                "updated_at": datetime.now()
            },
            {
                "order_id": "ORD005",
                "customer_info": {
                    "customer_id": "CUST005",
                    "name": "Marketing Agency Office",
                    "type": "corporate",
                    "contact": {
                        "email": "office@marketingagency.com",
                        "phone": "+1-555-5000",
                        "address": {
                            "street": "654 Creative Ave",
                            "city": "Downtown",
                            "state": "CA",
                            "zip": "90214"
                        }
                    },
                    "credit_limit": 15000,
                    "payment_terms": "net_15"
                },
                "employee_info": {
                    "employee_id": 9,  # Robert Taylor - Marketing Manager
                    "employee_name": "Robert Taylor",
                    "department": "Marketing",
                    "position": "Marketing Manager",
                    "email": "robert.taylor@company.com"
                },
                "order_details": {
                    "order_date": datetime.now() - timedelta(days=4),
                    "delivery_date": datetime.now() + timedelta(days=2),
                    "status": "confirmed",
                    "priority": "normal",
                    "delivery_method": "standard",
                    "special_instructions": "Healthy snacks for team meetings"
                },
                "items": [
                    {
                        "product_id": "PROD001",  # Organic Bananas
                        "quantity": 100,
                        "unit_price": 0.89,
                        "total_price": 89.00,
                        "discount_applied": 0.05,
                        "final_price": 84.55
                    },
                    {
                        "product_id": "PROD003",  # Greek Yogurt
                        "quantity": 80,
                        "unit_price": 1.79,
                        "total_price": 143.20,
                        "discount_applied": 0.06,
                        "final_price": 134.61
                    },
                    {
                        "product_id": "PROD004",  # Organic Spinach
                        "quantity": 20,
                        "unit_price": 1.29,
                        "total_price": 25.80,
                        "discount_applied": 0.07,
                        "final_price": 23.99
                    }
                ],
                "pricing": {
                    "subtotal": 258.00,
                    "tax_rate": 0.085,
                    "tax_amount": 21.93,
                    "shipping_cost": 15.00,
                    "total_amount": 294.93
                },
                "payment_info": {
                    "method": "bank_transfer",
                    "status": "paid",
                    "transaction_id": "TXN-2024-003"
                },
                "created_at": datetime.now() - timedelta(days=4),
                "updated_at": datetime.now()
            },
            {
                "order_id": "ORD006",
                "customer_info": {
                    "customer_id": "CUST006",
                    "name": "Engineering Team Lunch",
                    "type": "corporate",
                    "contact": {
                        "email": "lunch@engineering.com",
                        "phone": "+1-555-6000",
                        "address": {
                            "street": "987 Code St",
                            "city": "Tech Hub",
                            "state": "CA",
                            "zip": "90215"
                        }
                    },
                    "credit_limit": 10000,
                    "payment_terms": "net_30"
                },
                "employee_info": {
                    "employee_id": 1,  # John Smith - Senior Software Engineer
                    "employee_name": "John Smith",
                    "department": "Engineering",
                    "position": "Senior Software Engineer",
                    "email": "john.smith@company.com"
                },
                "order_details": {
                    "order_date": datetime.now() - timedelta(days=1),
                    "delivery_date": datetime.now() + timedelta(days=1),
                    "status": "processing",
                    "priority": "normal",
                    "delivery_method": "standard",
                    "special_instructions": "Team lunch order - keep fresh"
                },
                "items": [
                    {
                        "product_id": "PROD002",  # Whole Grain Bread
                        "quantity": 30,
                        "unit_price": 2.49,
                        "total_price": 74.70,
                        "discount_applied": 0.08,
                        "final_price": 68.72
                    },
                    {
                        "product_id": "PROD003",  # Greek Yogurt
                        "quantity": 40,
                        "unit_price": 1.79,
                        "total_price": 71.60,
                        "discount_applied": 0.06,
                        "final_price": 67.30
                    },
                    {
                        "product_id": "PROD005",  # Extra Virgin Olive Oil
                        "quantity": 5,
                        "unit_price": 7.99,
                        "total_price": 39.95,
                        "discount_applied": 0.08,
                        "final_price": 36.75
                    }
                ],
                "pricing": {
                    "subtotal": 186.25,
                    "tax_rate": 0.085,
                    "tax_amount": 15.83,
                    "shipping_cost": 10.00,
                    "total_amount": 212.08
                },
                "payment_info": {
                    "method": "credit_card",
                    "status": "pending",
                    "transaction_id": None
                },
                "created_at": datetime.now() - timedelta(days=1),
                "updated_at": datetime.now()
            }
        ]
        
        self.db.orders.insert_many(orders_data)
        logger.info(f"Created orders collection with {len(orders_data)} orders")
    
    def _create_indexes(self):
        """Create indexes for better query performance."""
        try:
            # Products collection indexes
            self.db.products.create_index("product_id", unique=True)
            self.db.products.create_index("category")
            self.db.products.create_index("brand")
            self.db.products.create_index("active")
            self.db.products.create_index("specifications.organic_certified")
            
            # Inventory collection indexes
            self.db.inventory.create_index("inventory_id", unique=True)
            self.db.inventory.create_index("product_id")
            self.db.inventory.create_index("warehouse_location.zone")
            self.db.inventory.create_index("stock_levels.current_stock")
            
            # Employees collection indexes
            self.db.employees.create_index("employee_id", unique=True)
            self.db.employees.create_index("department_id")
            self.db.employees.create_index("department_name")
            self.db.employees.create_index("manager_id")
            self.db.employees.create_index("position")
            self.db.employees.create_index("status")
            self.db.employees.create_index("email")
            
            # Orders collection indexes
            self.db.orders.create_index("order_id", unique=True)
            self.db.orders.create_index("customer_info.customer_id")
            self.db.orders.create_index("employee_info.employee_id")
            self.db.orders.create_index("employee_info.department")
            self.db.orders.create_index("employee_info.position")
            self.db.orders.create_index("order_details.status")
            self.db.orders.create_index("order_details.order_date")
            self.db.orders.create_index("payment_info.status")
            self.db.orders.create_index("items.product_id")
            
            logger.info("Created all database indexes successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get statistics about the database."""
        try:
            stats = {
                "database_name": self.db_name,
                "collections": {
                    "employees": self.db.employees.count_documents({}),
                    "products": self.db.products.count_documents({}),
                    "inventory": self.db.inventory.count_documents({}),
                    "orders": self.db.orders.count_documents({})
                },
                "total_documents": sum([
                    self.db.employees.count_documents({}),
                    self.db.products.count_documents({}),
                    self.db.inventory.count_documents({}),
                    self.db.orders.count_documents({})
                ])
            }
            return stats
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    
    def close_connection(self):
        """Close the MongoDB connection."""
        self.client.close()
        logger.info("MongoDB connection closed")

def main():
    """Main function to initialize the grocery warehouse database."""
    try:
        # Initialize database
        warehouse_db = GroceryWarehouseDB()
        
        # Create database and collections
        if warehouse_db.create_database():
            # Get and display database statistics
            stats = warehouse_db.get_database_stats()
            print("\n" + "="*50)
            print("GROCERY WAREHOUSE DATABASE CREATED SUCCESSFULLY")
            print("="*50)
            print(f"Database Name: {stats.get('database_name', 'N/A')}")
            print(f"Total Documents: {stats.get('total_documents', 0)}")
            print("\nCollection Statistics:")
            for collection, count in stats.get('collections', {}).items():
                print(f"  - {collection}: {count} documents")
            print("="*50)
            
            # Display sample data structure
            print("\nSample Data Structure:")
            print("- Employees: Employee data with department, position, salary info")
            print("- Products: Complex product info with specifications, pricing, supplier details")
            print("- Inventory: Stock levels, batch tracking, movement history, warehouse locations")
            print("- Orders: Customer info, employee relationships, order details, items, pricing, payment tracking")
            print("\nCross-Database Query Examples:")
            print("- Find employees who ordered products that are low in stock")
            print("- Show departments that ordered organic products")
            print("- Find project managers who placed large orders")
            print("- Compare ordering patterns between departments")
            print("- Analyze employee productivity vs order patterns")
            print("\nDatabase is ready for complex cross-database NoSQL agent operations!")
            
        else:
            print("Failed to create database. Please check MongoDB connection.")
            
    except Exception as e:
        print(f"Error: {e}")
        print("Please ensure MongoDB is running on localhost:27017")
    finally:
        if 'warehouse_db' in locals():
            warehouse_db.close_connection()

if __name__ == "__main__":
    main()
