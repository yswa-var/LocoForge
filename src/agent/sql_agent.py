import sqlite3
from typing import List, Dict, Any, Optional
from .llm_config import llm
import json
from datetime import datetime
from .logger import default_logger as logger

class SQLAgent:
    def __init__(self, db_path: str):
        self.db_path = db_path
        logger.info(f"Initializing SQLAgent with database: {db_path}")
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
    
    def _is_read_query(self, sql: str) -> bool:
        """Determine if a query is read-only."""
        normalized_sql = sql.strip().upper()
        return (normalized_sql.startswith("SELECT") or 
                normalized_sql.startswith("PRAGMA") or
                normalized_sql.startswith("EXPLAIN"))
        
    def _get_table_schema(self) -> str:
        """Get the schema of the database tables."""
        logger.debug("Fetching database schema")
        self.cursor.execute("SELECT sql FROM sqlite_master WHERE type='table'")
        schemas = self.cursor.fetchall()
        return "\n".join([schema[0] for schema in schemas])
    
    def _generate_sql_query(self, prompt: str) -> str:
        """Generate SQL query using LLM based on the prompt and schema."""
        logger.debug(f"Generating SQL query for prompt: {prompt}")
        schema = self._get_table_schema()
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
        
        response = llm.invoke(messages)
        generated_query = response.content.strip()
        logger.debug(f"Generated SQL query: {generated_query}")
        return generated_query
    
    def execute_query(self, prompt: str) -> Dict[str, Any]:
        """Execute a query based on the natural language prompt."""
        try:
            logger.info(f"Executing query for prompt: {prompt}")
            # Generate SQL query from the prompt
            sql_query = self._generate_sql_query(prompt)
            
            # Execute the query
            self.cursor.execute(sql_query)
            
            # Handle different types of queries
            if sql_query.strip().upper().startswith(('SELECT', 'PRAGMA')):
                results = self.cursor.fetchall()
                columns = [description[0] for description in self.cursor.description]
                logger.info(f"Query executed successfully. Retrieved {len(results)} rows")
                return {
                    "status": "success",
                    "query": sql_query,
                    "results": [dict(zip(columns, row)) for row in results]
                }
            else:
                # For INSERT, UPDATE, DELETE operations
                self.conn.commit()
                logger.info(f"Query executed successfully. Rows affected: {self.cursor.rowcount}")
                return {
                    "status": "success",
                    "query": sql_query,
                    "message": f"Query executed successfully. Rows affected: {self.cursor.rowcount}"
                }
                
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "query": sql_query if 'sql_query' in locals() else None,
                "error": str(e)
            }
    
    def close(self):
        """Close the database connection."""
        logger.info("Closing database connection")
        self.conn.close()

def main():
    """Main function to demonstrate the SQL agent usage."""
    try:
        # Initialize the agent
        agent = SQLAgent("/Users/yash/Documents/langgraph_as/src/agent/sales.db")
        
        # Example queries
        logger.info("Executing example queries")
        result = agent.execute_query("Show me all customers who have ordered more than 5 items in total, along with their total spending")
        print(result)
        
    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}", exc_info=True)
        print(f"An error occurred: {str(e)}")
    finally:
        agent.close()

if __name__ == "__main__":
    main()
