#!/usr/bin/env python3
"""
Test script to verify deployment setup
"""

import os
import sys
import sqlite3
from datetime import datetime

def test_database_initialization():
    """Test if database can be initialized"""
    print("Testing database initialization...")
    
    try:
        from sql_db_ops.sql_db_init import create_employee_management_db
        db_path = create_employee_management_db()
        print(f"✅ Database created successfully at: {db_path}")
        return True
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False

def test_database_connection():
    """Test database connection and basic queries"""
    print("Testing database connection...")
    
    try:
        db_path = "employee_management.db"
        if not os.path.exists(db_path):
            print(f"❌ Database file not found: {db_path}")
            return False
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Test basic queries
        cursor.execute("SELECT COUNT(*) FROM employees")
        employee_count = cursor.fetchone()[0]
        print(f"✅ Found {employee_count} employees")
        
        cursor.execute("SELECT COUNT(*) FROM departments")
        dept_count = cursor.fetchone()[0]
        print(f"✅ Found {dept_count} departments")
        
        cursor.execute("SELECT COUNT(*) FROM projects")
        project_count = cursor.fetchone()[0]
        print(f"✅ Found {project_count} projects")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection test failed: {e}")
        return False

def test_agent_import():
    """Test if agent can be imported"""
    print("Testing agent import...")
    
    try:
        from my_agent.agent import create_agent
        print("✅ Agent module imported successfully")
        return True
    except Exception as e:
        print(f"❌ Agent import failed: {e}")
        return False

def test_flask_app():
    """Test if Flask app can be created"""
    print("Testing Flask app creation...")
    
    try:
        from app import app
        print("✅ Flask app created successfully")
        return True
    except Exception as e:
        print(f"❌ Flask app creation failed: {e}")
        return False

def test_environment_variables():
    """Test environment variables"""
    print("Testing environment variables...")
    
    required_vars = ['OPENAPI_KEY', 'GOOGLE_API_KEY']
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"⚠️  Missing environment variables: {', '.join(missing_vars)}")
        print("   These are required for full functionality but won't prevent deployment")
        return True
    else:
        print("✅ All required environment variables are set")
        return True

def test_requirements():
    """Test if all required packages can be imported"""
    print("Testing package imports...")
    
    required_packages = [
        'flask',
        'gunicorn',
        'langgraph',
        'langchain',
        'openai',
        'google.generativeai',
        'pandas',
        'pymongo'
    ]
    
    failed_imports = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError as e:
            print(f"❌ {package}: {e}")
            failed_imports.append(package)
    
    if failed_imports:
        print(f"⚠️  Failed imports: {', '.join(failed_imports)}")
        return False
    else:
        print("✅ All packages imported successfully")
        return True

def main():
    """Run all tests"""
    print("=" * 50)
    print("DEPLOYMENT TEST SUITE")
    print("=" * 50)
    print(f"Timestamp: {datetime.now()}")
    print(f"Python version: {sys.version}")
    print(f"Working directory: {os.getcwd()}")
    print()
    
    tests = [
        ("Package Imports", test_requirements),
        ("Environment Variables", test_environment_variables),
        ("Database Initialization", test_database_initialization),
        ("Database Connection", test_database_connection),
        ("Agent Import", test_agent_import),
        ("Flask App", test_flask_app),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 50)
    print("TEST RESULTS SUMMARY")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Ready for deployment.")
        return 0
    else:
        print("⚠️  Some tests failed. Please fix issues before deploying.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 