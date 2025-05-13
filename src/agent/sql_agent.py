import sqlite3
from typing import List, Dict, Any, Optional
from src.utils.llm_config import llm
import json
from datetime import datetime
from src.utils.logger import logger
import asyncio
from contextlib import contextmanager

class SQLAgent:
    def __init__(self, db_path: str):
        """Initialize SQLAgent with database path and validate connection."""
        self.db_path = db_path
        logger.info("Initializing SQLAgent", extra={"db_path": db_path})
        # Validate database connection on initialization
        try:
            with self._get_connection() as conn:
                conn.execute("SELECT 1")
            logger.debug("Database connection validated successfully")
        except sqlite3.Error as e:
            logger.error("Failed to initialize database connection", extra={"error": str(e)})
            raise
    
    @contextmanager
    def _get_connection(self):
        """Create a new connection for each operation with proper error handling."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            logger.debug("Database connection established")
            yield conn
        except sqlite3.Error as e:
            logger.error("Database connection error", extra={"error": str(e)})
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Database connection closed")
    
    def _is_read_query(self, sql: str) -> bool:
        """Determine if a query is read-only with logging."""
        normalized_sql = sql.strip().upper()
        is_read = (normalized_sql.startswith("SELECT") or 
                  normalized_sql.startswith("PRAGMA") or
                  normalized_sql.startswith("EXPLAIN"))
        logger.debug("Query type determined", extra={"query": sql, "is_read": is_read})
        return is_read
        
    async def _get_table_schema(self) -> str:
        """Get the schema of the database tables with proper error handling."""
        logger.debug("Fetching database schema")
        
        def _fetch_schema():
            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table'")
                    schemas = cursor.fetchall()
                    schema_str = "\n".join([schema[0] for schema in schemas])
                    logger.debug("Schema fetched successfully", extra={"table_count": len(schemas)})
                    return schema_str
            except sqlite3.Error as e:
                logger.error("Failed to fetch schema", extra={"error": str(e)})
                raise
            
        return await asyncio.to_thread(_fetch_schema)
    
    async def _generate_sql_query(self, prompt: str) -> str:
        """Generate SQL query using LLM with proper error handling and logging."""
        logger.info("Generating SQL query", extra={"prompt": prompt})
        try:
            schema = await self._get_table_schema()
            system_prompt = f"""You are a SQL expert. Given the following database schema:
            {schema}
            
            Generate a valid SQL query based on the user's request. Only return the SQL query without any explanation.
            The query should be compatible with SQLite syntax.
            For date fields, use the format 'YYYY-MM-DD'.
            Make sure to properly escape string values and handle NULL values appropriately."""
            
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ]
            
            response = await asyncio.to_thread(llm.invoke, messages)
            generated_query = response.content.strip()
            logger.info("SQL query generated successfully", extra={"query": generated_query})
            return generated_query
        except Exception as e:
            logger.error("Failed to generate SQL query", extra={"error": str(e), "prompt": prompt})
            raise
    
    async def execute_query(self, prompt: str) -> Dict[str, Any]:
        """Execute a query based on the natural language prompt with comprehensive error handling."""
        logger.info("Executing query", extra={"prompt": prompt})
        sql_query = None
        
        try:
            # Generate SQL query from the prompt
            sql_query = await self._generate_sql_query(prompt)
            
            def _execute_and_fetch():
                try:
                    with self._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(sql_query)
                        
                        if self._is_read_query(sql_query):
                            results = cursor.fetchall()
                            columns = [description[0] for description in cursor.description]
                            logger.info("Read query executed successfully", 
                                      extra={"row_count": len(results), "columns": columns})
                            return {
                                "status": "success",
                                "query": sql_query,
                                "results": [dict(zip(columns, row)) for row in results]
                            }
                        else:
                            conn.commit()
                            logger.info("Write query executed successfully", 
                                      extra={"rows_affected": cursor.rowcount})
                            return {
                                "status": "success",
                                "query": sql_query,
                                "message": f"Query executed successfully. Rows affected: {cursor.rowcount}"
                            }
                except sqlite3.Error as e:
                    logger.error("Database error during query execution", 
                               extra={"error": str(e), "query": sql_query})
                    raise
            
            result = await asyncio.to_thread(_execute_and_fetch)
            return result
                
        except Exception as e:
            logger.error("Query execution failed", 
                        extra={"error": str(e), "query": sql_query, "prompt": prompt})
            return {
                "status": "error",
                "query": sql_query,
                "error": str(e)
            }
    
    def close(self):
        """Cleanup method for the SQLAgent."""
        logger.info("SQLAgent cleanup completed")
        pass

async def main():
    """Main function to demonstrate the SQL agent usage."""
    try:
        # Initialize the agent
        agent = SQLAgent("/Users/yash/Documents/langgraph_as/src/agent/sales.db")
        
        # Example queries
        logger.info("Executing example queries")
        result = await agent.execute_query("Get the top 5 customers by total purchase amount in the last quarter")
        print(result)
        
    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}", exc_info=True)
        print(f"An error occurred: {str(e)}")
    finally:
        agent.close()

if __name__ == "__main__":
    asyncio.run(main())
