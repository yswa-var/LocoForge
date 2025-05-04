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
        """Ensure database connection is established.
        
        Returns:
            sqlite3.Connection: Active database connection
            
        Raises:
            FileNotFoundError: If database file doesn't exist
            ConnectionError: If connection fails
        """
        try:
            if not self.sqlite_conn or not hasattr(self.sqlite_conn, 'cursor'):
                if not os.path.exists(self.sqlite_db_path):
                    raise FileNotFoundError(f"SQLite database {self.sqlite_db_path} does not exist. Run db_ops.py first")
                    
                self.sqlite_conn = sqlite3.connect(self.sqlite_db_path)
                # Enable foreign key support
                self.sqlite_conn.execute("PRAGMA foreign_keys = ON")
                # Use Row factory for better column access
                self.sqlite_conn.row_factory = sqlite3.Row
                
            return self.sqlite_conn
        except sqlite3.Error as e:
            raise ConnectionError(f"Failed to connect to SQLite database: {str(e)}")
    
    def _validate_table_name(self, table_name: str) -> bool:
        """Validate table name to prevent SQL injection.
        
        Args:
            table_name: The table name to validate
            
        Returns:
            bool: True if table exists, False otherwise
        """
        conn = self._ensure_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cursor.fetchone() is not None
    
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
    
    
    def delete(self, table: str, condition: str) -> Dict[str, Any]:
        """Delete records from the SQLite database.
        
        Args:
            table: Table name to delete from
            condition: WHERE clause condition (without 'WHERE' keyword)
        
        Returns:
            Dictionary with operation result and metadata
        """
        try:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            
            if not self._validate_table_name(table):
                return {"success": False, "error": f"Invalid table name: {table}"}
                
            query = f"DELETE FROM {table} WHERE {condition}"
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
        
    def list_tables(self) -> Dict[str, Any]:
        """List all tables in the database.
        
        Returns:
            Dictionary with list of tables
        """
        try:
            conn = self._ensure_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            return {
                "success": True,
                "tables": tables,
                "count": len(tables)
            }
        except Exception as e:
            return {"success": False, "error": f"Error listing tables: {str(e)}"}
    
    def close(self):
        if self.sqlite_conn:
            self.sqlite_conn.close()
            self.sqlite_conn = None
            
    def __enter__(self):
        """Support for context manager usage."""
        self._ensure_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close resources when exiting context manager."""
        self.close()
