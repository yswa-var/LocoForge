#!/usr/bin/env python3
"""
Setup script for Employees Database
Downloads and sets up the Neon employees sample database
"""

import os
import subprocess
import sys
from pathlib import Path

def check_dependencies():
    """Check if required tools are available"""
    required_tools = ['psql', 'pg_restore']
    missing_tools = []
    
    for tool in required_tools:
        try:
            subprocess.run([tool, '--version'], capture_output=True, check=True)
            print(f"✅ {tool} is available")
        except (subprocess.CalledProcessError, FileNotFoundError):
            missing_tools.append(tool)
            print(f"❌ {tool} is not available")
    
    if missing_tools:
        print(f"\n❌ Missing required tools: {', '.join(missing_tools)}")
        print("Please install PostgreSQL client tools:")
        print("  - macOS: brew install postgresql")
        print("  - Ubuntu: sudo apt-get install postgresql-client")
        print("  - Windows: Download from https://www.postgresql.org/download/windows/")
        return False
    
    return True

def setup_employees_database():
    """Set up the employees database"""
    
    # Database connection details
    db_url = os.getenv("POSTGRES_DB_URL")
    if not db_url:
        db_url = "postgresql://neondb_owner:npg_Td9jOSCDHrh1@ep-fragrant-snow-a8via4xi-pooler.eastus2.azure.neon.tech/employees?sslmode=require&channel_binding=require"
    
    # Extract database name from URL
    db_name = "employees"
    
    # Path to the employees.sql.gz file
    sql_file = Path("pgdb/employees.sql.gz")
    
    if not sql_file.exists():
        print(f"❌ Employees database file not found: {sql_file}")
        print("Please ensure the employees.sql.gz file is in the pgdb/ directory")
        return False
    
    print(f"📁 Found employees database file: {sql_file}")
    print(f"🗄️  Setting up database: {db_name}")
    
    try:
        # Create database if it doesn't exist
        print("🔧 Creating database...")
        create_db_cmd = [
            "psql", db_url.replace(f"/{db_name}", "/postgres"), 
            "-c", f"CREATE DATABASE {db_name};"
        ]
        
        result = subprocess.run(create_db_cmd, capture_output=True, text=True)
        if result.returncode != 0 and "already exists" not in result.stderr:
            print(f"⚠️  Database creation result: {result.stderr}")
        else:
            print("✅ Database created or already exists")
        
        # Create employees schema
        print("🔧 Creating employees schema...")
        schema_cmd = [
            "psql", db_url,
            "-c", "CREATE SCHEMA IF NOT EXISTS employees;"
        ]
        
        result = subprocess.run(schema_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Employees schema created")
        else:
            print(f"⚠️  Schema creation result: {result.stderr}")
        
        # Restore the database from the compressed file
        print("📦 Restoring database from employees.sql.gz...")
        restore_cmd = [
            "pg_restore", 
            "-d", db_url,
            "-Fc", str(sql_file),
            "-c", "-v", "--no-owner", "--no-privileges"
        ]
        
        result = subprocess.run(restore_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Database restored successfully")
        else:
            print(f"❌ Database restore failed: {result.stderr}")
            return False
        
        # Verify the setup
        print("🔍 Verifying database setup...")
        verify_cmd = [
            "psql", db_url,
            "-c", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'employees' ORDER BY table_name;"
        ]
        
        result = subprocess.run(verify_cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Database verification successful")
            print("📊 Tables found:")
            for line in result.stdout.strip().split('\n'):
                if line.strip() and not line.startswith('table_name') and not line.startswith('---'):
                    print(f"  - {line.strip()}")
        else:
            print(f"⚠️  Verification failed: {result.stderr}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False

def test_database_connection():
    """Test the database connection with a sample query"""
    print("\n🧪 Testing database connection...")
    
    db_url = os.getenv("POSTGRES_DB_URL")
    if not db_url:
        db_url = "postgresql://neondb_owner:npg_Td9jOSCDHrh1@ep-fragrant-snow-a8via4xi-pooler.eastus2.azure.neon.tech/employees?sslmode=require&channel_binding=require"
    
    # Test query from Neon documentation
    test_query = """
    SELECT d.dept_name, AVG(s.amount) AS average_salary
    FROM employees.salary s
    JOIN employees.dept_emp de ON s.employee_id = de.employee_id
    JOIN employees.department d ON de.department_id = d.id
    WHERE s.to_date > CURRENT_DATE AND de.to_date > CURRENT_DATE
    GROUP BY d.dept_name
    ORDER BY average_salary DESC
    LIMIT 5;
    """
    
    try:
        cmd = ["psql", db_url, "-c", test_query]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Database connection test successful")
            print("📊 Sample query results:")
            print(result.stdout)
        else:
            print(f"❌ Database connection test failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing database connection: {e}")
        return False
    
    return True

def main():
    """Main setup function"""
    print("🚀 Employees Database Setup")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Set up the database
    if not setup_employees_database():
        print("❌ Failed to set up employees database")
        sys.exit(1)
    
    # Test the connection
    if not test_database_connection():
        print("❌ Database connection test failed")
        sys.exit(1)
    
    print("\n🎉 Employees database setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Test the SQL agent: python test_sql_agent.py")
    print("2. Run the interactive chat: python my_agent/utils/sql_agent.py")
    print("3. Test the orchestrator: python test_orchestrator.py")

if __name__ == "__main__":
    main() 