from typing import Any, Dict, List, Union
from pymongo import MongoClient


class MongoDBExecutor:

    def __init__(self, mongo_uri="mongodb://localhost:27017", 
                 mongo_db="stock_data", 
                 mongo_collection="ohlc"):
        """Initialize MongoDB connection.
        
        Args:
            mongo_uri: MongoDB connection URI
            mongo_db: MongoDB database name
            mongo_collection: MongoDB collection name
        """
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_client = None
    
    def _ensure_connection(self):
        if not self.mongo_client:
            self.mongo_client = MongoClient(self.mongo_uri)
            self.mongo_client.server_info()  # Will raise an exception if connection fails
        
        return self.mongo_client[self.mongo_db][self.mongo_collection]
    
    
    def select(self, query: Dict) -> Dict[str, Any]:
        """Execute a find query on the MongoDB database.
        
        Args:
            query: MongoDB query dictionary with 'filter' and 'options'
        
        Returns:
            Dictionary with query results and metadata
        """
        try:
            collection = self._ensure_connection()
            query_filter = query.get("filter", {})
            query_options = query.get("options", {})
            sort_option = query_options.get("sort", None)
            limit_option = query_options.get("limit", None)
            project_option = query_options.get("projection", None)
            
            cursor = collection.find(query_filter, project_option) if project_option else collection.find(query_filter)
            
            if sort_option:
                cursor = cursor.sort(sort_option)
            
            if limit_option:
                cursor = cursor.limit(limit_option)
            
            results = []
            for doc in cursor:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                results.append(doc)
            
            return {
                "success": True,
                "query": query,
                "results": results,
                "count": len(results)
            }
        except Exception as e:
            return {"success": False, "error": f"Error executing MongoDB query: {str(e)}"}
    
    
    # def insert(self, documents: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
    #     """Insert document(s) into the MongoDB collection.
        
    #     Args:
    #         documents: Single document or list of documents to insert
        
    #     Returns:
    #         Dictionary with operation result and metadata
    #     """
    #     try:
    #         collection = self._ensure_connection()
            
    #         is_many = isinstance(documents, list)
            
    #         if is_many:
    #             result = collection.insert_many(documents)
    #             inserted_ids = [str(id) for id in result.inserted_ids]
    #             inserted_count = len(result.inserted_ids)
    #         else:
    #             result = collection.insert_one(documents)
    #             inserted_ids = [str(result.inserted_id)]
    #             inserted_count = 1
            
    #         return {
    #             "success": True,
    #             "operation": "insert_many" if is_many else "insert_one",
    #             "inserted_count": inserted_count,
    #             "inserted_ids": inserted_ids
    #         }
    #     except Exception as e:
    #         return {"success": False, "error": f"Error inserting into MongoDB: {str(e)}"}
    
    
    def update(self, filter_criteria: Dict[str, Any], update_data: Dict[str, Any], 
               update_many: bool = False, upsert: bool = False) -> Dict[str, Any]:
        """Update documents in the MongoDB collection.
        
        Args:
            filter_criteria: Filter to select documents to update
            update_data: Data to update (should include update operators like $set)
            update_many: If True, updates all matching documents, otherwise updates only the first match
            upsert: If True, creates a new document if no match is found
        
        Returns:
            Dictionary with operation result and metadata
        """
        try:
            collection = self._ensure_connection()
            
            if not any(key.startswith('$') for key in update_data.keys()):
                update_data = {"$set": update_data}
            
            if update_many:
                result = collection.update_many(filter_criteria, update_data, upsert=upsert)
                modified_count = result.modified_count
                matched_count = result.matched_count
                upserted_id = str(result.upserted_id) if result.upserted_id else None
            else:
                result = collection.update_one(filter_criteria, update_data, upsert=upsert)
                modified_count = result.modified_count
                matched_count = result.matched_count
                upserted_id = str(result.upserted_id) if result.upserted_id else None
            
            return {
                "success": True,
                "operation": "update_many" if update_many else "update_one",
                "matched_count": matched_count,
                "modified_count": modified_count,
                "upserted_id": upserted_id
            }
        except Exception as e:
            return {"success": False, "error": f"Error updating MongoDB: {str(e)}"}
    
    
    def delete(self, filter_criteria: Dict[str, Any], delete_many: bool = False) -> Dict[str, Any]:
        """Delete document(s) from the MongoDB collection.
        
        Args:
            filter_criteria: Filter to select documents to delete
            delete_many: If True, deletes all matching documents, otherwise deletes only the first match
        
        Returns:
            Dictionary with operation result and metadata
        """
        try:
            collection = self._ensure_connection()
            
            if delete_many:
                result = collection.delete_many(filter_criteria)
                deleted_count = result.deleted_count
            else:
                result = collection.delete_one(filter_criteria)
                deleted_count = result.deleted_count
            
            return {
                "success": True,
                "operation": "delete_many" if delete_many else "delete_one",
                "deleted_count": deleted_count
            }
        except Exception as e:
            return {"success": False, "error": f"Error deleting from MongoDB: {str(e)}"}
    
    def close(self):
        if self.mongo_client:
            self.mongo_client.close()
            self.mongo_client = None
