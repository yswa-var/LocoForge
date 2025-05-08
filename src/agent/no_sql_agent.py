import pymongo
from typing import List, Dict, Any, Optional, Union
import json
from datetime import datetime
from bson import ObjectId
from .llm_config import llm
from .logger import setup_logger

# Initialize logger
logger = setup_logger('nosql_agent')

# Custom JSON encoder for MongoDB ObjectId and datetime serialization
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class GeneralizedNoSQLAgent:
    def __init__(self, connection_string: str = "mongodb://localhost:27017/", database_name: Optional[str] = None):
        """
        Initialize the Generalized NoSQL agent with MongoDB connection.
        
        Args:
            connection_string (str): MongoDB connection string
            database_name (str, optional): Name of the database to connect to initially
        """
        logger.info(f"Initializing Generalized NoSQL agent with connection string: {connection_string}")
        self.client = pymongo.MongoClient(connection_string)
        self.current_db = None
        
        # If database_name is provided, connect to it
        if database_name:
            self.use_database(database_name)
            
        logger.info("Generalized NoSQL agent initialized successfully")
    
    def list_databases(self) -> List[str]:
        """
        List all available databases.
        
        Returns:
            List[str]: List of database names
        """
        logger.info("Listing all databases")
        return self.client.list_database_names()
    
    def use_database(self, database_name: str) -> bool:
        """
        Switch to a specific database.
        
        Args:
            database_name (str): Name of the database to use
            
        Returns:
            bool: Success status
        """
        logger.info(f"Switching to database: {database_name}")
        try:
            self.current_db = self.client[database_name]
            # Verify the database exists by listing collections
            self.current_db.list_collection_names()
            return True
        except Exception as e:
            logger.warning(f"Error accessing database {database_name}: {str(e)}")
            self.current_db = None
            return False
    
    def list_collections(self) -> List[str]:
        """
        List all collections in the current database.
        
        Returns:
            List[str]: List of collection names
        
        Raises:
            ValueError: If no database is selected
        """
        if self.current_db is None:
            raise ValueError("No database selected. Call use_database() first.")
            
        logger.info(f"Listing collections in database: {self.current_db.name}")
        return self.current_db.list_collection_names()
    
    def create_collection(self, collection_name: str) -> bool:
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
            self.current_db.create_collection(collection_name)
            return True
        except pymongo.errors.CollectionInvalid:
            logger.warning(f"Collection already exists: {collection_name}")
            return False
    
    def drop_collection(self, collection_name: str) -> bool:
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
            self.current_db.drop_collection(collection_name)
            return True
        except Exception as e:
            logger.error(f"Error dropping collection: {str(e)}")
            return False
    
    def get_collection_schema(self, collection_name: str) -> Dict[str, str]:
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
            
        if collection_name not in self.current_db.list_collection_names():
            raise ValueError(f"Collection does not exist: {collection_name}")
            
        logger.debug(f"Inferring schema for collection: {collection_name}")
        collection = self.current_db[collection_name]
        
        # Sample up to 100 documents to infer schema
        schema = {}
        for doc in collection.find().limit(100):
            for key, value in doc.items():
                if key not in schema:
                    schema[key] = type(value).__name__
        
        return schema
    
    def get_all_schemas(self) -> Dict[str, Dict[str, str]]:
        """
        Get schemas for all collections in the current database.
        
        Returns:
            Dict[str, Dict[str, str]]: Collection schemas
            
        Raises:
            ValueError: If no database is selected
        """
        if self.current_db is None:
            raise ValueError("No database selected. Call use_database() first.")
            
        logger.debug("Retrieving schemas for all collections")
        schemas = {}
        
        for collection_name in self.current_db.list_collection_names():
            try:
                schemas[collection_name] = self.get_collection_schema(collection_name)
            except Exception as e:
                logger.warning(f"Could not retrieve schema for collection {collection_name}: {str(e)}")
                
        return schemas

    def _generate_mongo_query(self, prompt: str) -> Dict[str, Any]:
        """
        Generate MongoDB query using LLM based on the prompt and current database schema.
        
        Args:
            prompt (str): Natural language description of the desired operation
            
        Returns:
            Dict[str, Any]: MongoDB query specification
            
        Raises:
            ValueError: If no database is selected
        """
        if self.current_db is None:
            raise ValueError("No database selected. Call use_database() first.")
            
        logger.info(f"Generating MongoDB query for prompt: {prompt}")
        
        # Get schemas to provide context for the LLM
        try:
            schemas = self.get_all_schemas()
        except Exception as e:
            logger.warning(f"Could not retrieve all schemas: {str(e)}")
            schemas = {}
        
        # Example operations for different query types
        examples = {
            "find": {
                "collection": "users",
                "operation": "find",
                "query": {"age": {"$gt": 30}}
            },
            "aggregate": {
                "collection": "orders",
                "operation": "aggregate",
                "pipeline": [
                    {"$group": {"_id": "$customer_id", "total": {"$sum": "$amount"}}}
                ]
            },
            "insert": {
                "collection": "products",
                "operation": "insert",
                "document": {
                    "name": "New Product",
                    "price": 29.99,
                    "category": "Electronics"
                }
            },
            "update": {
                "collection": "users",
                "operation": "update",
                "filter": {"email": "test@example.com"},
                "update": {"$set": {"status": "active"}}
            },
            "delete": {
                "collection": "orders",
                "operation": "delete",
                "filter": {"status": "cancelled"}
            }
        }
        
        # Construct available collections info
        collections_info = "\n".join([
            f"- {collection_name}: {list(schema.keys())}" 
            for collection_name, schema in schemas.items()
        ])
        
        system_prompt = f"""You are a MongoDB expert. Your task is to generate MongoDB queries based on natural language requests.

Current Database: {self.current_db.name}
Available Collections:
{collections_info}

Database Schema:
{json.dumps(schemas, indent=2)}

Response Format Requirements:
1. You must return ONLY a valid JSON object with no additional text or explanation
2. The JSON must follow this exact structure:
{{
    "collection": "string",  // Must be one of the available collections or a new collection name
    "operation": "string",   // Must be one of: ["find", "aggregate", "insert", "update", "delete"]
    "query": {{}},          // Required for "find" operation
    "pipeline": [],         // Required for "aggregate" operation
    "document": {{}},       // Required for "insert" operation
    "update": {{}},         // Required for "update" operation
    "filter": {{}}          // Required for "update" and "delete" operations
}}

Data Type Rules:
1. All field names must be strings
2. All values must be valid JSON values:
   - Strings: Use double quotes only
   - Numbers: Use decimal point for floats
   - Booleans: true or false (lowercase)
   - Null: null (lowercase)
   - Arrays: Use square brackets
   - Objects: Use curly braces
3. For dates:
   - Use ISO 8601 format strings: "YYYY-MM-DDTHH:mm:ss.sssZ"
   - Example: "2024-03-20T00:00:00.000Z"
   - For date ranges, use $gte (greater than or equal) and $lt (less than)
   - Do NOT use MongoDB-specific syntax like ISODate()
4. For ObjectIds:
   - Use string representation: "507f1f77bcf86cd799439011"
   - Do NOT use ObjectId() syntax

Example operations:
{json.dumps(examples, indent=2)}

Error Prevention:
1. Do not include any MongoDB-specific syntax in the JSON
2. Do not include any comments or explanations
3. Do not use single quotes for strings
4. Do not use trailing commas
5. Do not use undefined or NaN values
6. Do not use functions or expressions

Remember: The response must be a single, valid JSON object that can be parsed by json.loads().
If asked about database operations like 'create database', 'use database', 'list collections', etc.,
return a special operation type indicating this is a database management operation.

Special Operations:
- For database management, use operation="db_operation" and specify action="use_db|list_dbs|list_collections|etc."
Examples:
{{
  "operation": "db_operation",
  "action": "use_db",
  "database": "new_database"
}}
{{
  "operation": "db_operation",
  "action": "list_collections"
}}"""
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response = llm.invoke(messages)
            response_content = response.content.strip()
            
            # Try to parse the JSON response
            try:
                query_spec = json.loads(response_content)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse LLM response as JSON: {str(e)}")
                logger.error(f"Raw response: {response_content}")
                raise ValueError("Invalid JSON response from LLM")
            
            # Check for database operations
            if query_spec.get("operation") == "db_operation":
                return query_spec
            
            # Validate required fields for collection operations
            required_fields = ["collection", "operation"]
            missing_fields = [field for field in required_fields if field not in query_spec]
            if missing_fields:
                raise ValueError(f"Missing required fields in query specification: {missing_fields}")
            
            # Validate operation type
            valid_operations = ["find", "aggregate", "insert", "update", "delete"]
            if query_spec["operation"] not in valid_operations:
                raise ValueError(f"Invalid operation type: {query_spec['operation']}. Must be one of {valid_operations}")
            
            # Validate operation-specific fields
            operation = query_spec["operation"]
            if operation == "find" and "query" not in query_spec:
                raise ValueError("Find operation requires 'query' field")
            elif operation == "aggregate" and "pipeline" not in query_spec:
                raise ValueError("Aggregate operation requires 'pipeline' field")
            elif operation == "insert" and "document" not in query_spec:
                raise ValueError("Insert operation requires 'document' field")
            elif operation in ["update", "delete"] and "filter" not in query_spec:
                raise ValueError(f"{operation.capitalize()} operation requires 'filter' field")
            elif operation == "update" and "update" not in query_spec:
                raise ValueError("Update operation requires 'update' field")
            
            # Convert date strings to datetime objects if present
            def convert_dates(obj):
                if isinstance(obj, dict):
                    return {k: convert_dates(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_dates(item) for item in obj]
                elif isinstance(obj, str):
                    try:
                        # Try to parse as ISO format date
                        return datetime.fromisoformat(obj.replace('Z', '+00:00'))
                    except ValueError:
                        return obj
                return obj
            
            # Convert dates in the query specification
            query_spec = convert_dates(query_spec)
            
            logger.debug(f"Generated query specification: {json.dumps(query_spec, cls=MongoJSONEncoder, indent=2)}")
            return query_spec
            
        except Exception as e:
            logger.error(f"Error generating MongoDB query: {str(e)}", exc_info=True)
            raise

    def execute_query(self, prompt: str) -> Dict[str, Any]:
        """
        Execute a MongoDB query based on the natural language prompt.
        
        Args:
            prompt (str): Natural language description of the desired operation
            
        Returns:
            Dict[str, Any]: Query results and metadata
        """
        logger.info(f"Executing query for prompt: {prompt}")
        try:
            # Generate MongoDB query from the prompt
            query_spec = self._generate_mongo_query(prompt)
            
            # Check if this is a database operation
            if query_spec.get("operation") == "db_operation":
                return self._handle_db_operation(query_spec)
            
            # Ensure we have a database selected
            if self.current_db is None:
                return {
                    "status": "error",
                    "message": "No database selected. Use 'use database [name]' first."
                }
            
            collection_name = query_spec["collection"]
            
            # Create collection if it doesn't exist (for insert operations)
            if collection_name not in self.current_db.list_collection_names():
                if query_spec["operation"] == "insert":
                    logger.info(f"Collection {collection_name} does not exist. Creating it.")
                    self.create_collection(collection_name)
                else:
                    return {
                        "status": "error",
                        "message": f"Collection {collection_name} does not exist"
                    }
            
            collection = self.current_db[collection_name]
            logger.debug(f"Using collection: {collection_name}")
            
            # Execute the appropriate operation
            if query_spec["operation"] == "find":
                return self._execute_find(collection, query_spec)
                
            elif query_spec["operation"] == "aggregate":
                return self._execute_aggregate(collection, query_spec)
                
            elif query_spec["operation"] == "insert":
                return self._execute_insert(collection, query_spec)
                
            elif query_spec["operation"] == "update":
                return self._execute_update(collection, query_spec)
                
            elif query_spec["operation"] == "delete":
                return self._execute_delete(collection, query_spec)
                
            else:
                logger.error(f"Unsupported operation: {query_spec['operation']}")
                return {
                    "status": "error",
                    "message": f"Unsupported operation: {query_spec['operation']}"
                }
                
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "query_spec": query_spec if 'query_spec' in locals() else None
            }
    
    def _handle_db_operation(self, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle database management operations.
        
        Args:
            query_spec (Dict[str, Any]): Database operation specification
            
        Returns:
            Dict[str, Any]: Operation results
        """
        action = query_spec.get("action")
        
        if action == "use_db":
            database = query_spec.get("database")
            if not database:
                return {"status": "error", "message": "No database specified"}
            
            self.use_database(database)
            return {
                "status": "success",
                "message": f"Switched to database: {database}"
            }
            
        elif action == "list_dbs":
            databases = self.list_databases()
            return {
                "status": "success",
                "databases": databases,
                "count": len(databases)
            }
            
        elif action == "list_collections":
            if self.current_db is None:
                return {"status": "error", "message": "No database selected"}
            
            collections = self.list_collections()
            return {
                "status": "success",
                "database": self.current_db.name,
                "collections": collections,
                "count": len(collections)
            }
            
        elif action == "create_collection":
            if self.current_db is None:
                return {"status": "error", "message": "No database selected"}
                
            collection_name = query_spec.get("collection")
            if not collection_name:
                return {"status": "error", "message": "No collection name specified"}
                
            success = self.create_collection(collection_name)
            return {
                "status": "success" if success else "error",
                "message": f"Collection {collection_name} {'created' if success else 'already exists'}"
            }
            
        elif action == "drop_collection":
            if self.current_db is None:
                return {"status": "error", "message": "No database selected"}
                
            collection_name = query_spec.get("collection")
            if not collection_name:
                return {"status": "error", "message": "No collection name specified"}
                
            success = self.drop_collection(collection_name)
            return {
                "status": "success" if success else "error",
                "message": f"Collection {collection_name} {'dropped' if success else 'could not be dropped'}"
            }
            
        elif action == "get_schema":
            if self.current_db is None:
                return {"status": "error", "message": "No database selected"}
                
            collection_name = query_spec.get("collection")
            if collection_name:
                try:
                    schema = self.get_collection_schema(collection_name)
                    return {
                        "status": "success",
                        "collection": collection_name,
                        "schema": schema
                    }
                except ValueError as e:
                    return {"status": "error", "message": str(e)}
            else:
                try:
                    schemas = self.get_all_schemas()
                    return {
                        "status": "success",
                        "schemas": schemas
                    }
                except ValueError as e:
                    return {"status": "error", "message": str(e)}
                    
        else:
            return {
                "status": "error",
                "message": f"Unsupported database operation: {action}"
            }
    
    def _execute_find(self, collection, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a find operation."""
        logger.debug(f"Executing find operation with query: {query_spec['query']}")
        
        # Extract options if provided
        limit = query_spec.get("limit", 0)  # 0 means no limit
        skip = query_spec.get("skip", 0)
        sort = query_spec.get("sort", None)
        projection = query_spec.get("projection", None)
        
        # Execute the query with options
        cursor = collection.find(query_spec["query"], projection)
        
        # Apply options
        if skip > 0:
            cursor = cursor.skip(skip)
        if limit > 0:
            cursor = cursor.limit(limit)
        if sort:
            cursor = cursor.sort(sort)
            
        results = list(cursor)
        
        # Convert ObjectId to string for JSON serialization
        for result in results:
            if "_id" in result:
                result["_id"] = str(result["_id"])
                
        logger.info(f"Find operation completed. Found {len(results)} results")
        return {
            "status": "success",
            "operation": "find",
            "query": query_spec["query"],
            "results": results,
            "count": len(results)
        }
    
    def _execute_aggregate(self, collection, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an aggregate operation."""
        logger.debug(f"Executing aggregate operation with pipeline: {query_spec['pipeline']}")
        
        results = list(collection.aggregate(query_spec["pipeline"]))
        
        # Convert ObjectId to string for JSON serialization
        for result in results:
            if "_id" in result:
                result["_id"] = str(result["_id"])
                
        logger.info(f"Aggregate operation completed. Found {len(results)} results")
        return {
            "status": "success",
            "operation": "aggregate",
            "pipeline": query_spec["pipeline"],
            "results": results,
            "count": len(results)
        }
    
    def _execute_insert(self, collection, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an insert operation."""
        document = query_spec["document"]
        logger.debug(f"Executing insert operation with document: {document}")
        
        # Check if we're inserting a single document or multiple
        if isinstance(document, list):
            result = collection.insert_many(document)
            inserted_ids = [str(id) for id in result.inserted_ids]
            logger.info(f"Insert operation completed. Inserted {len(inserted_ids)} documents")
            return {
                "status": "success",
                "operation": "insert_many",
                "inserted_count": len(inserted_ids),
                "inserted_ids": inserted_ids
            }
        else:
            result = collection.insert_one(document)
            inserted_id = str(result.inserted_id)
            logger.info(f"Insert operation completed. Inserted ID: {inserted_id}")
            return {
                "status": "success",
                "operation": "insert_one",
                "document": document,
                "inserted_id": inserted_id
            }
    
    def _execute_update(self, collection, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an update operation."""
        filter_dict = query_spec["filter"]
        update_dict = query_spec["update"]
        upsert = query_spec.get("upsert", False)
        
        logger.debug(f"Executing update operation with filter: {filter_dict} and update: {update_dict}")
        
        # Check if we want to update one or many
        update_one = query_spec.get("update_one", False)
        if update_one:
            result = collection.update_one(filter_dict, update_dict, upsert=upsert)
            logger.info(f"Update one operation completed. Modified {result.modified_count} document")
            return {
                "status": "success",
                "operation": "update_one",
                "filter": filter_dict,
                "update": update_dict,
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
        else:
            result = collection.update_many(filter_dict, update_dict, upsert=upsert)
            logger.info(f"Update many operation completed. Modified {result.modified_count} documents")
            return {
                "status": "success",
                "operation": "update_many",
                "filter": filter_dict,
                "update": update_dict,
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            }
    
    def _execute_delete(self, collection, query_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a delete operation."""
        filter_dict = query_spec["filter"]
        logger.debug(f"Executing delete operation with filter: {filter_dict}")
        
        if not filter_dict:
            return {
                "status": "error",
                "message": "Delete filter cannot be empty",
                "query_spec": query_spec
            }
        
        # Check if we want to delete one or many
        delete_one = query_spec.get("delete_one", False)
        if delete_one:
            result = collection.delete_one(filter_dict)
            logger.info(f"Delete one operation completed. Deleted {result.deleted_count} document")
            return {
                "status": "success",
                "operation": "delete_one",
                "filter": filter_dict,
                "deleted_count": result.deleted_count
            }
        else:
            result = collection.delete_many(filter_dict)
            logger.info(f"Delete many operation completed. Deleted {result.deleted_count} documents")
            return {
                "status": "success",
                "operation": "delete_many",
                "filter": filter_dict,
                "deleted_count": result.deleted_count
            }
    
    def close(self):
        """Close the MongoDB connection."""
        logger.info("Closing MongoDB connection")
        self.client.close()

# def main():
#     """Main function to demonstrate the Generalized NoSQL agent usage with user_management_db."""
#     try:
#         # Initialize agent and connect to user_management_db
#         print("\n=== Initializing NoSQL Agent with user_management_db ===")
#         agent = GeneralizedNoSQLAgent()
#         result = agent.execute_query("use database user_management_db")
#         print("Database connection result:", json.dumps(result, indent=2, cls=MongoJSONEncoder))
        
#         # List available collections
#         print("\n=== Available Collections ===")
#         result = agent.execute_query("list all collections")
#         print("Collections:", json.dumps(result, indent=2, cls=MongoJSONEncoder))
        
#         # Test Case 1: Query Users
#         print("\n=== Test Case 1: Query Users ===")
#         print("Finding all users:")
#         result = agent.execute_query("Find all users")
#         print(json.dumps(result, indent=2, cls=MongoJSONEncoder))
        
#         print("\nFinding users with admin role:")
#         result = agent.execute_query("Find all users with role_name 'Admin'")
#         print(json.dumps(result, indent=2, cls=MongoJSONEncoder))
        
#         # Test Case 2: Query Roles
#         print("\n=== Test Case 2: Query Roles ===")
#         print("Finding all roles:")
#         result = agent.execute_query("Find all roles")
#         print(json.dumps(result, indent=2, cls=MongoJSONEncoder))
        
#         # Test Case 3: Query Activity Logs
#         print("\n=== Test Case 3: Query Activity Logs ===")
#         print("Finding recent login activities:")
#         result = agent.execute_query("Find all activity logs of type 'login'")
#         print(json.dumps(result, indent=2, cls=MongoJSONEncoder))
        
#         # Test Case 4: Complex Queries
#         print("\n=== Test Case 4: Complex Queries ===")
#         print("Finding active users in Engineering department:")
#         result = agent.execute_query("Find all users with status 'Active' and department 'Engineering'")
#         print(json.dumps(result, indent=2, cls=MongoJSONEncoder))
        
#         # Test Case 5: Aggregate Operations
#         print("\n=== Test Case 5: Aggregate Operations ===")
#         print("Calculating users per department:")
#         result = agent.execute_query("Calculate the number of users in each department")
#         print(json.dumps(result, indent=2, cls=MongoJSONEncoder))
        
#         # Test Case 6: Schema Information
#         print("\n=== Test Case 6: Schema Information ===")
#         print("Getting users collection schema:")
#         result = agent.execute_query("Get the schema of the users collection")
#         print(json.dumps(result, indent=2, cls=MongoJSONEncoder))
        
#         # Cleanup
#         print("\n=== Cleanup ===")
#         agent.close()
#         print("Test completed and connection closed.")
            
#     except Exception as e:
#         print(f"An error occurred during testing: {str(e)}")
#         logger.error(f"An error occurred: {str(e)}", exc_info=True)
#         if 'agent' in locals():
#             agent.close()

# if __name__ == "__main__":
#     main()