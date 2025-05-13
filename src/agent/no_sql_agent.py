"""NoSQL agent implementation."""

import logging
import pymongo
from typing import List, Dict, Any, Optional
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import weakref
import re

logger = logging.getLogger(__name__)

class GeneralizedNoSQLAgent:
    def __init__(self, connection_string: str = "mongodb://localhost:27017/", database_name: Optional[str] = "user_management_db"):
        """
        Initialize the Generalized NoSQL agent with MongoDB connection.
        
        Args:
            connection_string (str): MongoDB connection string
            database_name (str, optional): Name of the database to connect to initially
        """
        logger.info(f"Initializing Generalized NoSQL agent with connection string: {connection_string}")
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.current_db = None
        self._loop = None
        self._initialized = False
            
        logger.info("Generalized NoSQL agent initialized successfully")

    async def initialize(self):
        """Initialize the database connection."""
        if self._initialized:
            return

        try:
            self._loop = asyncio.get_running_loop()
            logger.info(f"Attempting to connect to MongoDB at {self.connection_string}")
            
            # Set explicit client options to avoid container detection
            client_options = {
                "maxPoolSize": 50,
                "minPoolSize": 10,
                "maxIdleTimeMS": 30000,
                "waitQueueTimeoutMS": 10000,
                "connectTimeoutMS": 20000,
                "serverSelectionTimeoutMS": 5000,
                "retryWrites": True,
                "retryReads": True,
                "directConnection": False,
                "appname": "query_orchestrator"
            }
            
            # Test connection first
            test_client = AsyncIOMotorClient(self.connection_string, **client_options)
            await test_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB server")
            
            # Now create the main client
            self.client = AsyncIOMotorClient(self.connection_string, **client_options)
            
            # Set the database directly without calling use_database
            if self.database_name:
                logger.info(f"Attempting to access database: {self.database_name}")
                self.current_db = self.client[self.database_name]
                # Verify the database exists by listing collections
                collections = await self.current_db.list_collection_names()
                logger.info(f"Found collections in database: {collections}")
                
                # If no collections exist, create the users collection
                if not collections and self.database_name == "user_management_db":
                    logger.info("Creating users collection")
                    await self.current_db.create_collection("users")
                    # Insert a test user
                    test_user = {
                        "_id": "09d0645e-57a0-4dec-995f-be4e3997d945",
                        "username": "testuser",
                        "email": "test@example.com"
                    }
                    await self.current_db.users.insert_one(test_user)
                    logger.info("Created test user in users collection")
            
            self._initialized = True
            logger.info("Database initialization completed successfully")
            
        except Exception as e:
            logger.error(f"Error initializing MongoDB connection: {str(e)}")
            self._cleanup()
            raise

    def _cleanup(self):
        """Clean up resources."""
        self._initialized = False
        self._loop = None
        if self.client:
            self.client.close()
            self.client = None
        self.current_db = None

    async def _ensure_initialized(self):
        """Ensure the agent is initialized and connected."""
        if not self._initialized:
            await self.initialize()
        elif self._loop != asyncio.get_running_loop():
            # If we're in a different event loop, reinitialize
            self._cleanup()
            await self.initialize()

    async def list_databases(self) -> List[str]:
        """
        List all available databases.
        
        Returns:
            List[str]: List of database names
        """
        await self._ensure_initialized()
        logger.info("Listing all databases")
        return await self.client.list_database_names()
    
    async def use_database(self, database_name: str) -> bool:
        """
        Switch to a specific database.
        
        Args:
            database_name (str): Name of the database to use
            
        Returns:
            bool: Success status
        """
        await self._ensure_initialized()
        logger.info(f"Switching to database: {database_name}")
        try:
            self.current_db = self.client[database_name]
            # Verify the database exists by listing collections
            await self.current_db.list_collection_names()
            return True
        except Exception as e:
            logger.warning(f"Error accessing database {database_name}: {str(e)}")
            self.current_db = None
            return False
    
    async def list_collections(self) -> List[str]:
        """
        List all collections in the current database.
        
        Returns:
            List[str]: List of collection names
        
        Raises:
            ValueError: If no database is selected
        """
        await self._ensure_initialized()
        if self.current_db is None:
            raise ValueError("No database selected. Call use_database() first.")
            
        logger.info(f"Listing collections in database: {self.current_db.name}")
        return await self.current_db.list_collection_names()
    
    async def create_collection(self, collection_name: str) -> bool:
        """
        Create a new collection in the current database.
        
        Args:
            collection_name (str): Name of the collection to create
            
        Returns:
            bool: Success status
            
        Raises:
            ValueError: If no database is selected
        """
        if self.current_db is None:
            raise ValueError("No database selected. Call use_database() first.")
            
        logger.info(f"Creating collection: {collection_name}")
        try:
            await self.current_db.create_collection(collection_name)
            return True
        except pymongo.errors.CollectionInvalid:
            logger.warning(f"Collection already exists: {collection_name}")
            return False
    
    async def drop_collection(self, collection_name: str) -> bool:
        """
        Drop a collection from the current database.
        
        Args:
            collection_name (str): Name of the collection to drop
            
        Returns:
            bool: Success status
            
        Raises:
            ValueError: If no database is selected
        """
        if self.current_db is None:
            raise ValueError("No database selected. Call use_database() first.")
            
        logger.info(f"Dropping collection: {collection_name}")
        try:
            await self.current_db.drop_collection(collection_name)
            return True
        except Exception as e:
            logger.error(f"Error dropping collection: {str(e)}")
            return False
    
    async def get_collection_schema(self, collection_name: str) -> Dict[str, str]:
        """
        Infer the schema of a collection from sample documents.
        
        Args:
            collection_name (str): Name of the collection
            
        Returns:
            Dict[str, str]: Inferred schema with field names and types
            
        Raises:
            ValueError: If no database is selected or collection doesn't exist
        """
        if self.current_db is None:
            raise ValueError("No database selected. Call use_database() first.")
            
        if collection_name not in await self.current_db.list_collection_names():
            raise ValueError(f"Collection does not exist: {collection_name}")
            
        logger.debug(f"Inferring schema for collection: {collection_name}")
        collection = self.current_db[collection_name]
        
        # Sample up to 100 documents to infer schema
        schema = {}
        async for doc in collection.find().limit(100):
            for key, value in doc.items():
                if key not in schema:
                    schema[key] = type(value).__name__
        
        return schema
    
    async def get_all_schemas(self) -> Dict[str, Any]:
        """
        Get schemas for all collections in the current database.
        
        Returns:
            Dict[str, Any]: Dictionary of collection schemas
        """
        if not self.current_db:
            raise ValueError("No database selected")
            
        schemas = {}
        collections = await self.current_db.list_collection_names()
        
        for collection_name in collections:
            collection = self.current_db[collection_name]
            # Get a sample document to infer schema
            sample = await collection.find_one()
            if sample:
                schemas[collection_name] = self._infer_schema(sample)
                
        return schemas

    def _infer_schema(self, document: Dict[str, Any]) -> Dict[str, str]:
        """
        Infer schema from a document.
        
        Args:
            document (Dict[str, Any]): Sample document
            
        Returns:
            Dict[str, str]: Schema mapping field names to types
        """
        schema = {}
        for key, value in document.items():
            schema[key] = type(value).__name__
        return schema

    async def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a query on the current database.
        
        Args:
            query (str): Query to execute
            
        Returns:
            Dict[str, Any]: Query results
        """
        await self._ensure_initialized()
        if self.current_db is None:
            raise ValueError("No database selected")
            
        try:
            # Parse the query to determine the collection and operation
            query_lower = query.lower()
            
            # Determine collection based on query content
            collection_name = None
            if "user" in query_lower:
                collection_name = "users"
            elif "customer" in query_lower:
                collection_name = "customers"
            elif "product" in query_lower:
                collection_name = "products"
            elif "order" in query_lower:
                collection_name = "orders"
                
            if not collection_name:
                return {
                    "status": "error",
                    "error": "Could not determine collection from query"
                }
                
            collection = self.current_db[collection_name]
            
            # Extract query parameters and projection
            query_params = {}
            projection = None
            
            if "user" in query_lower:
                # Extract user_id from query
                user_id_match = re.search(r"'([^']+)'", query)
                if user_id_match:
                    query_params["user_id"] = user_id_match.group(1)
                
                # If query mentions specific fields, create projection
                if "username" in query_lower or "email" in query_lower:
                    projection = {}
                    if "username" in query_lower:
                        projection["username"] = 1
                    if "email" in query_lower:
                        projection["email"] = 1
                    # Always include user_id in projection
                    projection["user_id"] = 1
            
            # Build the query based on the task definition
            if "order details" in query_lower:
                # For order details, we need to join with customers and products
                pipeline = [
                    {
                        "$lookup": {
                            "from": "customers",
                            "localField": "customer_id",
                            "foreignField": "_id",
                            "as": "customer"
                        }
                    },
                    {
                        "$unwind": "$customer"
                    },
                    {
                        "$lookup": {
                            "from": "products",
                            "localField": "items.product_id",
                            "foreignField": "_id",
                            "as": "product"
                        }
                    },
                    {
                        "$unwind": "$product"
                    },
                    {
                        "$project": {
                            "order_id": "$_id",
                            "customer_name": "$customer.name",
                            "product_name": "$product.name",
                            "quantity": "$items.quantity",
                            "price": "$items.price",
                            "order_date": 1
                        }
                    }
                ]
                
                # Execute the aggregation pipeline
                cursor = collection.aggregate(pipeline)
                results = await cursor.to_list(length=None)
                
            else:
                # For simple queries, find documents matching the parameters
                cursor = collection.find(query_params, projection)
                results = await cursor.to_list(length=None)
            
            return {
                "status": "success",
                "collection": collection_name,
                "results": results,
                "count": len(results)
            }
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }

    async def close(self):
        """Close the database connection."""
        self._cleanup()
        logger.info("Database connection closed")

async def main():
    """Main function to demonstrate the Generalized NoSQL agent usage with db."""
    try:
        # Initialize agent and connect to db
        agent = GeneralizedNoSQLAgent()
        await agent.initialize()
        
        # List available collections
        print("\n=== Available Collections ===")
        collections = await agent.list_collections()
        print("Collections:", collections)
        
        # Test the user query
        print("\n=== Testing User Query ===")
        result = await agent.execute_query("Find the username and email of the user with user_id '09d0645e-57a0-4dec-995f-be4e3997d945'.")
        print("Query Result:", result)
            
    except Exception as e:
        print(f"An error occurred during testing: {str(e)}")
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        if 'agent' in locals():
            await agent.close()

if __name__ == "__main__":
    asyncio.run(main())