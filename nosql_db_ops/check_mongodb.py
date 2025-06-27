#!/usr/bin/env python3
"""
MongoDB Connection Checker
This script checks if MongoDB is running and provides installation instructions if needed.
"""

import socket
import subprocess
import sys
import platform

def check_mongodb_connection():
    """Check if MongoDB is running on localhost:27017."""
    try:
        # Try to connect to MongoDB
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', 27017))
        sock.close()
        
        if result == 0:
            print("✅ MongoDB is running on localhost:27017")
            return True
        else:
            print("❌ MongoDB is not running on localhost:27017")
            return False
            
    except Exception as e:
        print(f"❌ Error checking MongoDB connection: {e}")
        return False

def get_installation_instructions():
    """Get MongoDB installation instructions based on the operating system."""
    system = platform.system().lower()
    
    print("\n" + "="*60)
    print("MONGODB INSTALLATION INSTRUCTIONS")
    print("="*60)
    
    if system == "darwin":  # macOS
        print("\n📋 For macOS:")
        print("1. Install MongoDB using Homebrew:")
        print("   brew tap mongodb/brew")
        print("   brew install mongodb-community")
        print("\n2. Start MongoDB service:")
        print("   brew services start mongodb/brew/mongodb-community")
        print("\n3. Or start manually:")
        print("   mongod --config /usr/local/etc/mongod.conf")
        
    elif system == "linux":
        print("\n📋 For Linux (Ubuntu/Debian):")
        print("1. Import MongoDB public GPG key:")
        print("   wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -")
        print("\n2. Create list file for MongoDB:")
        print("   echo 'deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse' | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list")
        print("\n3. Install MongoDB:")
        print("   sudo apt-get update")
        print("   sudo apt-get install -y mongodb-org")
        print("\n4. Start MongoDB service:")
        print("   sudo systemctl start mongod")
        print("   sudo systemctl enable mongod")
        
    elif system == "windows":
        print("\n📋 For Windows:")
        print("1. Download MongoDB Community Server from:")
        print("   https://www.mongodb.com/try/download/community")
        print("\n2. Run the installer and follow the setup wizard")
        print("\n3. Start MongoDB service:")
        print("   net start MongoDB")
        print("\n4. Or start manually:")
        print("   'C:\\Program Files\\MongoDB\\Server\\6.0\\bin\\mongod.exe'")
        
    else:
        print(f"\n📋 For {system}:")
        print("Please visit https://docs.mongodb.com/manual/installation/")
        print("for installation instructions specific to your operating system.")
    
    print("\n" + "="*60)
    print("ALTERNATIVE: Use Docker")
    print("="*60)
    print("If you have Docker installed, you can run MongoDB in a container:")
    print("\n1. Pull MongoDB image:")
    print("   docker pull mongo:latest")
    print("\n2. Run MongoDB container:")
    print("   docker run -d -p 27017:27017 --name mongodb mongo:latest")
    print("\n3. Stop container when done:")
    print("   docker stop mongodb")
    print("   docker rm mongodb")

def check_python_dependencies():
    """Check if required Python packages are installed."""
    try:
        import pymongo
        print("✅ PyMongo is installed")
        return True
    except ImportError:
        print("❌ PyMongo is not installed")
        print("\n📋 Install required packages:")
        print("   pip install -r requirements.txt")
        return False

def main():
    """Main function to check MongoDB setup."""
    print("🔍 Checking MongoDB Setup...")
    print("="*40)
    
    # Check Python dependencies
    deps_ok = check_python_dependencies()
    
    # Check MongoDB connection
    mongodb_ok = check_mongodb_connection()
    
    print("\n" + "="*40)
    print("SUMMARY")
    print("="*40)
    
    if deps_ok and mongodb_ok:
        print("✅ Everything is ready!")
        print("You can now run:")
        print("   python nosql_db_init.py")
        print("   python test_queries.py")
    else:
        if not deps_ok:
            print("❌ Python dependencies need to be installed")
        if not mongodb_ok:
            print("❌ MongoDB needs to be installed and started")
            get_installation_instructions()
    
    print("\n" + "="*40)

if __name__ == "__main__":
    main() 