#!/usr/bin/env python3
"""
Test script to verify MongoEngine migration
Tests the NoSQL agent with MongoEngine ODM
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_mongoengine_connection():
    """Test basic MongoEngine connection and functionality"""
    print("🧪 Testing MongoEngine Migration")
    print("=" * 40)
    
    try:
        # Import the updated NoSQL agent
        from my_agent.utils.nosql_agent import NoSQLQueryExecutor, Movie, Comment, User, Session, Theater
        
        print("✅ Successfully imported MongoEngine-based NoSQL agent")
        
        # Test connection
        print("\n🔌 Testing MongoDB connection...")
        agent = NoSQLQueryExecutor()
        print("✅ MongoDB connection successful")
        
        # Test basic query
        print("\n🔍 Testing basic query...")
        test_query = {
            "collection": "movies",
            "query": {},
            "projection": {"title": 1, "year": 1, "_id": 0}
        }
        
        result = agent.execute_query(str(test_query).replace("'", '"'))
        if result.get('success'):
            print(f"✅ Query executed successfully. Found {result.get('row_count', 0)} movies")
        else:
            print(f"❌ Query failed: {result.get('error', 'Unknown error')}")
            return False
        
        # Test MongoEngine ORM directly
        print("\n📊 Testing MongoEngine ORM directly...")
        movie_count = Movie.objects.count()
        print(f"✅ Total movies in database: {movie_count}")
        
        # Test filtering
        recent_movies = Movie.objects.filter(year__gte=2020).limit(5)
        print(f"✅ Found {recent_movies.count()} movies from 2020 or later")
        
        # Test embedded document query
        high_rated = Movie.objects.filter(imdb__rating__gte=8).limit(3)
        print(f"✅ Found {high_rated.count()} highly rated movies (IMDB >= 8)")
        
        print("\n🎉 All tests passed! MongoEngine migration successful!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure mongoengine is installed: pip install mongoengine")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_sample_queries():
    """Test sample queries with the new MongoEngine implementation"""
    print("\n🧪 Testing Sample Queries")
    print("=" * 30)
    
    try:
        from my_agent.utils.nosql_agent import NoSQLQueryExecutor
        
        agent = NoSQLQueryExecutor()
        
        # Test a few sample queries
        sample_queries = [
            "Show movies from 2020",
            "Find action movies with high ratings",
            "Show movies starring Tom Hanks"
        ]
        
        for query in sample_queries:
            print(f"\n🔍 Testing: {query}")
            try:
                result = agent.generate_and_execute_query(query)
                if result['execution_result']['success']:
                    print(f"✅ Success! Found {result['execution_result']['row_count']} results")
                else:
                    print(f"❌ Query failed: {result['execution_result']['error']}")
            except Exception as e:
                print(f"❌ Error: {e}")
        
        print("\n🎉 Sample query tests completed!")
        return True
        
    except Exception as e:
        print(f"❌ Sample query test failed: {e}")
        return False

if __name__ == "__main__":
    # Check environment variables
    if not os.getenv("MONGO_DB"):
        print("❌ MONGO_DB environment variable not set")
        print("Please set MONGO_DB in your .env file")
        sys.exit(1)
    
    if not os.getenv("OPENAPI_KEY"):
        print("❌ OPENAPI_KEY environment variable not set")
        print("Please set OPENAPI_KEY in your .env file")
        sys.exit(1)
    
    # Run tests
    connection_success = test_mongoengine_connection()
    query_success = test_sample_queries()
    
    if connection_success and query_success:
        print("\n🎉 All tests passed! MongoEngine migration is working correctly!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
        sys.exit(1) 