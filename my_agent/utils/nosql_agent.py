#!/usr/bin/env python3
"""
NoSQL Query Executor using OpenAI and MongoEngine
Executes MongoDB queries against the sample_mflix database using MongoEngine ODM
Returns structured JSON output with query and results
"""

import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from dotenv import load_dotenv
from mongoengine import connect, Document, StringField, IntField, ListField, DateTimeField, ReferenceField, EmbeddedDocumentField, EmbeddedDocument, FloatField, DictField
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage
import logging
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("NoSQLAgent")

# MongoEngine Document Models
class ImdbInfo(EmbeddedDocument):
    """Embedded document for IMDB information"""
    rating = FloatField()
    votes = IntField()
    id = IntField()

class TomatoesViewer(EmbeddedDocument):
    """Embedded document for Tomatoes viewer ratings"""
    rating = FloatField()
    numReviews = IntField()
    meter = IntField()

class TomatoesCritic(EmbeddedDocument):
    """Embedded document for Tomatoes critic ratings"""
    rating = FloatField()
    numReviews = IntField()
    meter = IntField()

class TomatoesInfo(EmbeddedDocument):
    """Embedded document for Tomatoes information"""
    viewer = EmbeddedDocumentField(TomatoesViewer)
    critic = EmbeddedDocumentField(TomatoesCritic)
    fresh = IntField()
    rotten = IntField()
    production = StringField()

class Awards(EmbeddedDocument):
    """Embedded document for awards information"""
    wins = IntField()
    nominations = IntField()
    text = StringField()

class Address(EmbeddedDocument):
    """Embedded document for address information"""
    street1 = StringField()
    city = StringField()
    state = StringField()
    zipcode = StringField()

class GeoLocation(EmbeddedDocument):
    """Embedded document for geolocation"""
    type = StringField()
    coordinates = ListField(FloatField())

class Location(EmbeddedDocument):
    """Embedded document for location information"""
    address = EmbeddedDocumentField(Address)
    geo = EmbeddedDocumentField(GeoLocation)

class Movie(Document):
    """Movie document model"""
    title = StringField(required=True, max_length=200)
    year = IntField()
    genres = ListField(StringField(max_length=50))
    cast = ListField(StringField())
    directors = ListField(StringField())
    writers = ListField(StringField())
    plot = StringField()
    fullplot = StringField()
    runtime = IntField()
    released = DateTimeField()
    countries = ListField(StringField())
    languages = ListField(StringField())
    poster = StringField()
    type = StringField()
    imdb = EmbeddedDocumentField(ImdbInfo)
    tomatoes = EmbeddedDocumentField(TomatoesInfo)
    awards = EmbeddedDocumentField(Awards)
    num_mflix_comments = IntField()
    
    meta = {
        'collection': 'movies',
        'allow_inheritance': True
    }

class Comment(Document):
    """Comment document model"""
    name = StringField()
    email = StringField()
    movie_id = ReferenceField(Movie)
    text = StringField()
    date = DateTimeField()
    
    meta = {
        'collection': 'comments'
    }

class User(Document):
    """User document model"""
    name = StringField()
    email = StringField()
    password = StringField()
    
    meta = {
        'collection': 'users'
    }

class Session(Document):
    """Session document model"""
    user_id = StringField()
    jwt = StringField()
    
    meta = {
        'collection': 'sessions'
    }

class Theater(Document):
    """Theater document model"""
    theaterId = IntField()
    location = EmbeddedDocumentField(Location)
    
    meta = {
        'collection': 'theaters'
    }

class NoSQLQueryExecutor:
    """NoSQL Query Executor for Sample Mflix Database using MongoEngine"""
    
    def __init__(self, connection_string: str = None):
        """
        Initialize the NoSQL Query Executor
        
        Args:
            connection_string: MongoDB connection string (defaults to MONGO_DB from .env)
        """
        self.connection_string = connection_string or os.getenv("MONGO_DB")
        if not self.connection_string:
            raise ValueError("MongoDB connection string not found. Set MONGO_DB in .env file")
        
        # Connect to MongoDB using MongoEngine
        try:
            # Extract database name from connection string
            db_name = "sample_mflix"
            if "mongodb+srv://" in self.connection_string:
                # For Atlas connections, we need to specify the database name
                connect(db=db_name, host=self.connection_string)
            else:
                # For local connections
                connect(db=db_name, host=self.connection_string)
            
            print("Successfully connected to MongoDB using MongoEngine!")
            
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")
            raise e
        
        self.model = ChatOpenAI(
            model="gpt-4o-mini",
            api_key=os.getenv("OPENAPI_KEY")
        )
        
        # Database schema context
        self.db_context = self._build_database_context()
        
    def _build_database_context(self) -> str:
        """Build concise database context for sample_mflix schema using MongoEngine models"""
        context = """
MONGODB DATABASE SCHEMA FOR SAMPLE_MFLIX (MongoEngine Models)

DATABASE: sample_mflix

1. MOVIES COLLECTION (Movie Document):
   - title: String (required, max 200 chars)
   - year: Integer
   - genres: List[String]
   - cast: List[String]
   - directors: List[String]
   - writers: List[String]
   - plot: String
   - fullplot: String
   - runtime: Integer
   - released: DateTime
   - countries: List[String]
   - languages: List[String]
   - poster: String (URL)
   - type: String
   - imdb: EmbeddedDocument (rating: Float, votes: Integer, id: Integer)
   - tomatoes: EmbeddedDocument (viewer/critic ratings, fresh/rotten counts)
   - awards: EmbeddedDocument (wins: Integer, nominations: Integer, text: String)
   - num_mflix_comments: Integer

2. COMMENTS COLLECTION (Comment Document):
   - name: String
   - email: String
   - movie_id: ReferenceField(Movie)
   - text: String
   - date: DateTime

3. USERS COLLECTION (User Document):
   - name: String
   - email: String
   - password: String

4. SESSIONS COLLECTION (Session Document):
   - user_id: String
   - jwt: String

5. THEATERS COLLECTION (Theater Document):
   - theaterId: Integer
   - location: EmbeddedDocument (address: Address, geo: GeoLocation)

MongoEngine Query Patterns:
- Movie.objects.filter(year=2020)
- Movie.objects.filter(genres__in=['Action'], imdb__rating__gte=7)
- Movie.objects.filter(cast__in=['Tom Hanks'])
- Movie.objects.filter(directors__in=['Christopher Nolan'])
- Movie.objects.filter(released__gte=datetime(1990,1,1), released__lte=datetime(1999,12,31))
- Movie.objects.filter(runtime__gte=120)
- Movie.objects.filter(imdb__rating__gte=8).order_by('-imdb__rating')
- Movie.objects.filter(awards__wins__gte=1)
- Comment.objects.filter(movie_id__in=Movie.objects.filter(genres='Action'))
- Aggregation: Movie.objects.aggregate([...])
- Text search: Movie.objects.search_text('search term')
- Geospatial: Theater.objects.filter(location__geo__near=[longitude, latitude])
"""
        return context
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a MongoDB query using MongoEngine and return results
        """
        logger.info(f"Received query: {query}")
        start_time = time.time()
        try:
            if query.strip().startswith('['):
                logger.info("Detected aggregation pipeline.")
                pipeline = json.loads(query)
                results = []
                collections_to_try = [Movie, Comment, User, Session, Theater]
                for collection_class in collections_to_try:
                    try:
                        logger.info(f"Trying aggregation on collection: {collection_class.__name__}")
                        results = list(collection_class.objects.aggregate(pipeline))
                        if results:
                            logger.info(f"Aggregation returned {len(results)} results from {collection_class.__name__}")
                            break
                    except Exception as agg_e:
                        logger.warning(f"Aggregation failed on {collection_class.__name__}: {agg_e}")
                        continue
            else:
                logger.info("Detected find query.")
                query_dict = json.loads(query)
                collection_name = query_dict.get('collection', 'movies')
                find_query = query_dict.get('query', {})
                projection = query_dict.get('projection', {})
                logger.info(f"Collection: {collection_name}, Query: {find_query}, Projection: {projection}")
                collection_map = {
                    'movies': Movie,
                    'comments': Comment,
                    'users': User,
                    'sessions': Session,
                    'theaters': Theater
                }
                document_class = collection_map.get(collection_name, Movie)
                queryset = document_class.objects(**find_query)
                # Remove '_id' from projection for MongoEngine compatibility
                if projection and '_id' in projection:
                    logger.info("Removing '_id' from projection for MongoEngine compatibility.")
                    projection.pop('_id')
                if projection:
                    fields = {}
                    exclude_fields = {}
                    for field, value in projection.items():
                        if value == 1:
                            fields[field] = 1
                        elif value == 0:
                            exclude_fields[field] = 0
                    if fields:
                        queryset = queryset.only(*fields.keys())
                    if exclude_fields:
                        queryset = queryset.exclude(*exclude_fields.keys())
                results = list(queryset)
                logger.info(f"Find query returned {len(results)} results.")
            def convert_to_dict(obj):
                if hasattr(obj, 'to_mongo'):
                    doc_dict = obj.to_mongo().to_dict()
                    if '_id' in doc_dict:
                        doc_dict['_id'] = str(doc_dict['_id'])
                    return doc_dict
                elif isinstance(obj, dict):
                    return {k: convert_to_dict(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_to_dict(item) for item in obj]
                elif hasattr(obj, '__class__') and obj.__class__.__name__ == 'ObjectId':
                    return str(obj)
                elif isinstance(obj, datetime):
                    return obj.isoformat()
                else:
                    return obj
            converted_results = convert_to_dict(results)
            elapsed = time.time() - start_time
            logger.info(f"Query execution completed in {elapsed:.2f} seconds.")
            return {
                "success": True,
                "query": query,
                "row_count": len(converted_results),
                "data": converted_results,
                "execution_time_seconds": elapsed
            }
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Query failed after {elapsed:.2f} seconds: {e}")
            return {
                "success": False,
                "query": query,
                "error": str(e),
                "data": [],
                "execution_time_seconds": elapsed
            }
    
    def generate_and_execute_query(self, prompt: str) -> Dict[str, Any]:
        """
        Generate MongoDB query from natural language prompt and execute it using MongoEngine
        
        Args:
            prompt: Natural language description of what data to retrieve
            
        Returns:
            Structured response with generated query and results
        """
        # Create system prompt with database context
        system_prompt = """
You are a MongoDB query generator for the Sample Mflix Database using MongoEngine ODM. 

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
        # MongoEngine handles connection cleanup automatically
        pass

def create_nosql_agent() -> NoSQLQueryExecutor:
    """Create a pre-configured NoSQL Query Executor"""
    return NoSQLQueryExecutor()

def interactive_nosql_chat():
    """Run an interactive NoSQL query session"""
    print("🎬 NoSQL Query Executor (Sample Mflix Database) - MongoEngine Edition")
    print("=" * 60)
    
    try:
        agent = create_nosql_agent()
        print("✅ Connected to MongoDB successfully using MongoEngine")
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
