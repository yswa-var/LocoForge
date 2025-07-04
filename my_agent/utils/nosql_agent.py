#!/usr/bin/env python3
"""
NoSQL Query Executor using OpenAI
Executes MongoDB queries against the sample_mflix database
Returns structured JSON output with query and results
"""

import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage

# Load environment variables
load_dotenv()

class NoSQLQueryExecutor:
    """NoSQL Query Executor for Sample Mflix Database"""
    
    def __init__(self, connection_string: str = None):
        """
        Initialize the NoSQL Query Executor
        
        Args:
            connection_string: MongoDB connection string (defaults to MONGO_DB from .env)
        """
        self.connection_string = connection_string or os.getenv("MONGO_DB")
        if not self.connection_string:
            raise ValueError("MongoDB connection string not found. Set MONGO_DB in .env file")
        
        # Create a new client and connect to the server
        try:
            # Use the recommended MongoDB Atlas connection method
            self.client = MongoClient(
                self.connection_string, 
                server_api=ServerApi('1')
            )
            
            # Send a ping to confirm a successful connection
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
            
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")
            print("Trying alternative connection method...")
            
            # Try alternative connection with minimal settings
            try:
                self.client = MongoClient(self.connection_string)
                self.client.admin.command('ping')
                print("Successfully connected with alternative method!")
            except Exception as e2:
                print(f"Alternative connection also failed: {e2}")
                raise e2
        
        self.db_name = "sample_mflix"
        self.db = self.client[self.db_name]
        
        self.model = ChatOpenAI(
            model="gpt-4o-mini",
            api_key=os.getenv("OPENAPI_KEY")
        )
        
        # Database schema context
        self.db_context = self._build_database_context()
        
    def _build_database_context(self) -> str:
        """Build concise database context for sample_mflix schema (movies, comments, users, sessions, theaters)"""
        context = """
MONGODB DATABASE SCHEMA FOR SAMPLE_MFLIX

DATABASE: sample_mflix

1. MOVIES COLLECTION:
   - _id: ObjectId
   - title: String
   - year: Number
   - genres: Array[String]
   - cast: Array[String]
   - directors: Array[String]
   - writers: Array[String]
   - plot: String
   - fullplot: String
   - runtime: Number
   - released: ISODate
   - countries: Array[String]
   - languages: Array[String]
   - poster: String (URL)
   - type: String (e.g., 'movie')
   - imdb: { rating: Number, votes: Number, id: Number }
   - tomatoes: { 
       viewer: { rating: Number, numReviews: Number, meter: Number },
       critic: { rating: Number, numReviews: Number, meter: Number },
       fresh: Number, rotten: Number, production: String
     }
   - awards: { wins: Number, nominations: Number, text: String }
   - num_mflix_comments: Number

2. COMMENTS COLLECTION:
   - _id: ObjectId
   - name: String
   - email: String
   - movie_id: ObjectId (references movies._id)
   - text: String
   - date: ISODate

3. USERS COLLECTION:
   - _id: ObjectId
   - name: String
   - email: String
   - password: String (hashed)

4. SESSIONS COLLECTION:
   - _id: ObjectId
   - user_id: String
   - jwt: String

5. THEATERS COLLECTION:
   - _id: ObjectId
   - theaterId: Number
   - location: {
       address: { street1: String, city: String, state: String, zipcode: String },
       geo: { type: String, coordinates: [Number, Number] }
     }

6. EMBEDDED_MOVIES COLLECTION:
   - Same as movies but with plot_embedding field for vector search

RELATIONSHIPS:
- comments.movie_id references movies._id
- sessions.user_id references users._id
- users can have multiple comments

COMMON QUERY PATTERNS:
- $lookup to join comments with movies or users
- $match for filtering (genre, year, cast, director, etc.)
- $group for aggregations (average ratings, count by genre, awards)
- $unwind for array operations (genres, cast, directors)
- $project for field selection
- $sort for ordering results
- $limit for result limiting
- Date range queries with $gte, $lte
- Array element matching with $elemMatch
- Nested object queries using dot notation
- Geospatial queries on theaters.location.geo
- Text search on movies using $text
"""
        return context
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a MongoDB query and return results
        
        Args:
            query: MongoDB query (JSON string or aggregation pipeline)
            
        Returns:
            Dictionary with query results and metadata
        """
        try:
            # Parse the query
            if query.strip().startswith('['):
                # Aggregation pipeline
                pipeline = json.loads(query)
                # Try different collections for aggregation
                collections_to_try = ['movies', 'comments', 'users', 'sessions', 'theaters', 'embedded_movies']
                results = []
                
                for collection_name in collections_to_try:
                    try:
                        results = list(self.db[collection_name].aggregate(pipeline))
                        if results:
                            break
                    except Exception:
                        continue
                        
            else:
                # Find query
                query_dict = json.loads(query)
                collection_name = query_dict.get('collection', 'movies')
                find_query = query_dict.get('query', {})
                projection = query_dict.get('projection', {})
                
                collection = getattr(self.db, collection_name)
                results = list(collection.find(find_query, projection))
            
            # Convert ObjectId to string for JSON serialization
            def convert_objectid(obj):
                if isinstance(obj, dict):
                    return {k: convert_objectid(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_objectid(item) for item in obj]
                elif hasattr(obj, '__class__') and obj.__class__.__name__ == 'ObjectId':
                    return str(obj)
                elif isinstance(obj, datetime):
                    return obj.isoformat()
                else:
                    return obj
            
            converted_results = convert_objectid(results)
            
            return {
                "success": True,
                "query": query,
                "row_count": len(converted_results),
                "data": converted_results
            }
            
        except Exception as e:
            return {
                "success": False,
                "query": query,
                "error": str(e),
                "data": []
            }
    
    def generate_and_execute_query(self, prompt: str) -> Dict[str, Any]:
        """
        Generate MongoDB query from natural language prompt and execute it
        
        Args:
            prompt: Natural language description of what data to retrieve
            
        Returns:
            Structured response with generated query and results
        """
        # Create system prompt with database context
        system_prompt = """
You are a MongoDB query generator for the Sample Mflix Database (movie database). 

""" + self.db_context + """

INSTRUCTIONS:
1. Generate ONLY valid MongoDB queries (find operations) or aggregation pipelines
2. Use appropriate $lookup for joining collections (e.g., comments with movies)
3. Use $match for filtering conditions (genre, year, cast, director, etc.)
4. Use $project for field selection
5. Use $sort for meaningful ordering
6. Use $limit to limit results (default 20 if not specified)
7. Return ONLY the MongoDB query in JSON format, no explanations

FOR FIND QUERIES, return format:
{
  "collection": "collection_name",
  "query": { "field": "value" },
  "projection": { "field": 1, "_id": 0 }
}

FOR AGGREGATION PIPELINES, return format:
[
  { "$match": { "field": "value" } },
  { "$lookup": { "from": "collection", "localField": "field", "foreignField": "field", "as": "alias" } },
  { "$project": { "field": 1 } }
]

EXAMPLE PROMPTS AND QUERIES:
- "Show all movies": { "collection": "movies", "query": {}, "projection": { "title": 1, "year": 1, "genres": 1, "_id": 0 } }
- "Movies from 2020": { "collection": "movies", "query": { "year": 2020 }, "projection": { "title": 1, "year": 1, "_id": 0 } }
- "Action movies with high ratings": [{ "$match": { "genres": "Action", "imdb.rating": { "$gte": 7 } } }, { "$project": { "title": 1, "imdb.rating": 1, "year": 1, "_id": 0 } }, { "$sort": { "imdb.rating": -1 } }, { "$limit": 20 }]
- "Movies with comments": [{ "$lookup": { "from": "comments", "localField": "_id", "foreignField": "movie_id", "as": "comments" } }, { "$match": { "comments": { "$ne": [] } } }, { "$project": { "title": 1, "comment_count": { "$size": "$comments" }, "_id": 0 } }, { "$sort": { "comment_count": -1 } }]
- "Top rated directors": [{ "$unwind": "$directors" }, { "$group": { "_id": "$directors", "avg_rating": { "$avg": "$imdb.rating" }, "movie_count": { "$sum": 1 } } }, { "$match": { "avg_rating": { "$gte": 7 } } }, { "$sort": { "avg_rating": -1 } }, { "$limit": 10 }]
"""
        
        # Generate MongoDB query
        messages = [
            HumanMessage(content=f"System: {system_prompt}"),
            HumanMessage(content=f"Generate MongoDB query for: {prompt}")
        ]
        
        response = self.model.invoke(messages)
        generated_query = response.content.strip()
        
        # Clean up the query (remove markdown if present)
        if generated_query.startswith("```json"):
            generated_query = generated_query.replace("```json", "").replace("```", "").strip()
        elif generated_query.startswith("```"):
            generated_query = generated_query.replace("```", "").strip()
        
        # Execute the generated query
        query_result = self.execute_query(generated_query)
        
        # Return structured response
        return {
            "prompt": prompt,
            "generated_mongodb_query": generated_query,
            "execution_result": query_result,
            "timestamp": self._get_timestamp()
        }
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        return datetime.now().isoformat()
    
    def get_sample_queries(self) -> List[str]:
        """Get sample query prompts for testing"""
        return [
            "Show all movies from 2020",
            "Find action movies with high ratings",
            "Show movies with the most comments",
            "List top rated directors",
            "Find movies starring Tom Hanks",
            "Show movies with awards",
            "List movies by genre",
            "Find movies released in the 1990s",
            "Show movies with high IMDB ratings",
            "Find movies with specific cast members",
            "Show movies with plot summaries",
            "List movies by country",
            "Find movies with specific directors",
            "Show movies with runtime over 2 hours",
            "Find movies with specific awards"
        ]
    
    def close_connection(self):
        """Close the MongoDB connection"""
        if self.client:
            self.client.close()

def create_nosql_agent() -> NoSQLQueryExecutor:
    """Create a pre-configured NoSQL Query Executor"""
    return NoSQLQueryExecutor()

def interactive_nosql_chat():
    """Run an interactive NoSQL query session"""
    print("🎬 NoSQL Query Executor (Sample Mflix Database)")
    print("=" * 50)
    
    try:
        agent = create_nosql_agent()
        print("✅ Connected to MongoDB successfully")
        print("📊 Sample Mflix database context loaded")
        
        print("\n💡 Sample queries you can try:")
        sample_queries = agent.get_sample_queries()
        for i, query in enumerate(sample_queries[:5], 1):
            print(f"{i}. {query}")
        
        print("\nType 'quit' to exit, 'samples' to see more examples")
        print("-" * 50)
        
        while True:
            try:
                user_input = input("\n🔍 Enter your query: ").strip()
                
                if user_input.lower() == 'quit':
                    print("👋 Goodbye!")
                    break
                elif user_input.lower() == 'samples':
                    print("\n📋 Sample Queries:")
                    for i, query in enumerate(sample_queries, 1):
                        print(f"{i}. {query}")
                    continue
                elif not user_input:
                    continue
                
                # Generate and execute query
                print("🤖 Generating MongoDB query...")
                result = agent.generate_and_execute_query(user_input)
                
                # Display results in structured format
                print("\n📊 RESULTS:")
                print(json.dumps(result, indent=2, default=str))
                
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}")
                
    except Exception as e:
        print(f"❌ Failed to initialize NoSQL agent: {e}")
        print("Make sure MONGO_DB and OPENAPI_KEY are set in your .env file")
    finally:
        if 'agent' in locals():
            agent.close_connection()

if __name__ == "__main__":
    # Check if required environment variables are available
    if not os.getenv("OPENAPI_KEY"):
        print("❌ Error: OPENAPI_KEY not found in environment variables")
        print("Please make sure your .env file contains: OPENAPI_KEY=your_api_key_here")
        exit(1)
    
    if not os.getenv("MONGO_DB"):
        print("❌ Error: MONGO_DB not found in environment variables")
        print("Please make sure your .env file contains: MONGO_DB=mongodb+srv://anton:<db_password>@cluster0.ku0y7rt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        exit(1)
    
    interactive_nosql_chat()
