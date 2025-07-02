#!/usr/bin/env python3
"""
SQL Query Executor using OpenAI
Executes SQL queries against the employee management database
Returns structured JSON output with query and results
"""

import os
import sqlite3
import json
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from openai import OpenAI
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage

# Load environment variables
load_dotenv()

class SQLQueryExecutor:
    """SQL Query Executor for Employee Management Database"""
    
    def __init__(self, db_path: str = None):
        """
        Initialize the SQL Query Executor
        
        Args:
            db_path: Path to the SQLite database (defaults to SQL_DB from .env)
        """
        self.db_path = db_path or os.getenv("SQL_DB")
        if not self.db_path:
            raise ValueError("Database path not found. Set SQL_DB in .env file")
        
        self.model = ChatOpenAI(
            model="gpt-4o-mini",
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        
        # Database schema context
        self.db_context = self._build_database_context()
        print(self.db_context)
        
    def _build_database_context(self) -> str:
        """Build comprehensive database context by dynamically querying the database schema"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            
            context_parts = ["DATABASE SCHEMA FOR EMPLOYEE MANAGEMENT SYSTEM:\n"]
            
            # Build context for each table
            for table in tables:
                table_name = table[0]
                
                # Get table schema
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                # Get foreign key information
                cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                foreign_keys = cursor.fetchall()
                
                # Build table description
                context_parts.append(f"{len(context_parts)}. {table_name.upper()} TABLE:")
                
                for col in columns:
                    col_id, col_name, col_type, not_null, default_val, pk = col
                    
                    # Find foreign key info for this column
                    fk_info = ""
                    for fk in foreign_keys:
                        if fk[3] == col_name:  # Column name in foreign key
                            fk_info = f" (FOREIGN KEY -> {fk[1]}.{fk[4]})"
                            break
                    
                    # Build column description
                    col_desc = f"   - {col_name} ({col_type}"
                    if pk:
                        col_desc += ", PRIMARY KEY"
                    if not_null:
                        col_desc += ", NOT NULL"
                    if default_val is not None:
                        col_desc += f", DEFAULT {default_val}"
                    col_desc += fk_info + ")"
                    
                    context_parts.append(col_desc)
                
                context_parts.append("")  # Empty line between tables
            
            # Build relationships section
            context_parts.append("RELATIONSHIPS:")
            relationships = []
            
            for table in tables:
                table_name = table[0]
                cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                foreign_keys = cursor.fetchall()
                
                for fk in foreign_keys:
                    fk_table = fk[1]  # Referenced table
                    fk_column = fk[4]  # Referenced column
                    local_column = fk[3]  # Local column
                    
                    # Determine relationship type (simplified)
                    if fk_table == table_name:
                        rel_type = "self-referencing"
                    else:
                        rel_type = "many-to-one"
                    
                    relationships.append(f"- {table_name}.{local_column} -> {fk_table}.{fk_column} ({rel_type})")
            
            if relationships:
                context_parts.extend(relationships)
            else:
                context_parts.append("- No foreign key relationships found")
            
            # Add common query patterns
            context_parts.extend([
                "",
                "COMMON QUERY PATTERNS:",
                "- Use JOINs to get related data from multiple tables",
                "- Use WHERE clauses for filtering",
                "- Use ORDER BY for meaningful sorting",
                "- Use LIMIT for result size control",
                "- Use aggregate functions (COUNT, SUM, AVG, etc.) for analysis",
                "- Use GROUP BY for grouped aggregations"
            ])
            
            conn.close()
            
            return "\n".join(context_parts)
            
        except Exception as e:
            # Fallback to basic context if schema querying fails
            return f"""
DATABASE SCHEMA FOR EMPLOYEE MANAGEMENT SYSTEM:

Note: Unable to dynamically load schema due to error: {str(e)}
Using fallback context - consider checking database connection and permissions.

Basic tables typically include:
- employees
- departments  
- projects
- employee_projects
- attendance

Please ensure your database is accessible and contains the expected tables.
"""
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a SQL query and return results
        
        Args:
            query: SQL query to execute
            
        Returns:
            Dictionary with query results and metadata
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Execute query
            cursor.execute(query)
            
            # Get column names
            columns = [description[0] for description in cursor.description] if cursor.description else []
            
            # Get results
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = []
            for row in results:
                data.append(dict(zip(columns, row)))
            
            conn.close()
            
            return {
                "success": True,
                "query": query,
                "columns": columns,
                "row_count": len(data),
                "data": data
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
        Generate SQL query from natural language prompt and execute it
        
        Args:
            prompt: Natural language description of what data to retrieve
            
        Returns:
            Structured response with generated query and results
        """
        # Create system prompt with database context
        system_prompt = f"""
You are a SQL query generator for an Employee Management System. 

{self.db_context}

INSTRUCTIONS:
1. Generate ONLY valid SQLite SQL queries
2. Use appropriate JOINs to get related data
3. Include proper WHERE clauses for filtering
4. Use ORDER BY for meaningful sorting
5. Limit results to reasonable amounts (use LIMIT 50 if not specified)
6. Return ONLY the SQL query, no explanations

EXAMPLE PROMPTS AND QUERIES:
- "Show all employees": SELECT * FROM employees LIMIT 50
- "Employees in IT department": SELECT e.*, d.department_name FROM employees e JOIN departments d ON e.department_id = d.department_id WHERE d.department_name = 'IT'
- "Average salary by department": SELECT d.department_name, AVG(e.salary) as avg_salary FROM employees e JOIN departments d ON e.department_id = d.department_id GROUP BY d.department_id, d.department_name
"""
        
        # Generate SQL query
        messages = [
            HumanMessage(content=f"System: {system_prompt}"),
            HumanMessage(content=f"Generate SQL query for: {prompt}")
        ]
        
        response = self.model.invoke(messages)
        generated_query = response.content.strip()
        
        # Clean up the query (remove markdown if present)
        if generated_query.startswith("```sql"):
            generated_query = generated_query.replace("```sql", "").replace("```", "").strip()
        elif generated_query.startswith("```"):
            generated_query = generated_query.replace("```", "").strip()
        
        # Execute the generated query
        query_result = self.execute_query(generated_query)
        
        # Return structured response
        return {
            "prompt": prompt,
            "generated_sql": generated_query,
            "execution_result": query_result,
            "timestamp": self._get_timestamp()
        }
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def get_sample_queries(self) -> List[str]:
        """Get sample query prompts for testing"""
        return [
            "Show all employees with their department names",
            "Find employees with salary above 50000",
            "Show projects and their assigned employees",
            "Calculate average salary by department",
            "Find employees who are managers",
            "Show attendance records for the last 7 days",
            "List departments with their total budget",
            "Find employees working on multiple projects",
            "Show project managers and their projects",
            "Calculate total hours worked by employee"
        ]

def create_sql_agent() -> SQLQueryExecutor:
    """Create a pre-configured SQL Query Executor"""
    return SQLQueryExecutor()

def interactive_sql_chat():
    """Run an interactive SQL query session"""
    print("🗄️  SQL Query Executor (OpenAI)")
    print("=" * 50)
    
    try:
        agent = create_sql_agent()
        print("✅ Connected to database successfully")
        print("📊 Database context loaded")
        
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
                print("🤖 Generating SQL query...")
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
        print(f"❌ Failed to initialize SQL agent: {e}")
        print("Make sure SQL_DB and OPENAI_API_KEY are set in your .env file")

if __name__ == "__main__":
    # Check if required environment variables are available
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY not found in environment variables")
        print("Please make sure your .env file contains: OPENAI_API_KEY=your_api_key_here")
        exit(1)
    
    if not os.getenv("SQL_DB"):
        print("❌ Error: SQL_DB not found in environment variables")
        print("Please make sure your .env file contains: SQL_DB=/path/to/your/database.db")
        exit(1)
    
    interactive_sql_chat()
