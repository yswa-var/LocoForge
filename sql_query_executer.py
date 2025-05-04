import os
import sqlite3
from typing import Any, Dict
import pandas as pd

class SQLiteExecutor:
    
    def __init__(self, sqlite_db_path="ohlc.db"):
        """Initialize SQLite database connection.
        
        Args:
            sqlite_db_path: Path to the SQLite database file
        """
        self.sqlite_db_path = sqlite_db_path
        self.sqlite_conn = None

    def query_creator(self, prompt: str) -> str:
        """Create a SQL query based on the user's prompt.
        
        Args:
            prompt: User's natural language query
        
        Returns:
            SQL query string
        """
        # TODO: Implement query creation logic
        
        return "SELECT * FROM ohlc"
    
    def _ensure_connection(self):
        if not self.sqlite_conn or not hasattr(self.sqlite_conn, 'cursor'):
            if not os.path.exists(self.sqlite_db_path):
                raise FileNotFoundError(f"SQLite database {self.sqlite_db_path} does not exist. Run db_ops.py first")
            self.sqlite_conn = sqlite3.connect(self.sqlite_db_path)
        return self.sqlite_conn
    
    def select(self, query: str) -> Dict[str, Any]:
        """Execute a SELECT query on the SQLite database.
        
        Args:
            query: SQL SELECT query string 
        
        Returns:
            Dictionary with query results and metadata
        """
        try:
            conn = self._ensure_connection()
            
            if not query.strip().upper().startswith("SELECT"):
                return {"success": False, "error": "Only SELECT queries are allowed"}
            
            df = pd.read_sql_query(query, conn)
            results = df.to_dict(orient='records')
            
            return {
                "success": True,
                "query": query,
                "results": results,
                "row_count": len(results)
            }
        except Exception as e:
            return {"success": False, "error": f"Error executing SQLite query: {str(e)}"}
    
    
    def update(self, table: str, data: Dict[str, Any], condition: str) -> Dict[str, Any]:
        """Update records in the SQLite database.
        
        Args:
            table: Table name
            data: Dictionary of column:value pairs to update
            condition: WHERE clause (without 'WHERE' keyword)
        
        Returns:
            Dictionary with operation result and metadata
        """
        try:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            
            set_clause = ', '.join([f"{col} = ?" for col in data.keys()])
            values = list(data.values())
            
            query = f"UPDATE {table} SET {set_clause} WHERE {condition}"
            
            cursor.execute(query, values)
            conn.commit()
            
            return {
                "success": True,
                "operation": "update",
                "table": table,
                "affected_rows": cursor.rowcount,
                "condition": condition
            }
        except Exception as e:
            return {"success": False, "error": f"Error updating SQLite: {str(e)}"}
    
    
    def delete(self, query: str, table: str) -> Dict[str, Any]:
        """Delete records from the SQLite database.
        
        Args:
            query: SQL DELETE query string 
        
        Returns:
            Dictionary with operation result and metadata
        """
        try:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            
            cursor.execute(query)
            conn.commit()
            
            return {
                "success": True,
                "operation": "delete",
                "table": table,
                "affected_rows": cursor.rowcount,
            }
        except Exception as e:
            return {"success": False, "error": f"Error deleting from SQLite: {str(e)}"}
    
    def close(self):
        if self.sqlite_conn:
            self.sqlite_conn.close()
            self.sqlite_conn = None
