#!/usr/bin/env python3
"""
SQL Query Executor using OpenAI
Executes SQL queries against the employee management database
Returns structured JSON output with query and results
"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from openai import OpenAI
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage

# Load environment variables
load_dotenv()

class SQLQueryExecutor:
    """SQL Query Executor for Employee Management Database"""
    
    def __init__(self, db_url: str = None):
        """
        Initialize the SQL Query Executor
        
        Args:
            db_url: PostgreSQL connection URL (defaults to POSTGRES_DB_URL from .env)
        """
        self.db_url = db_url or os.getenv("POSTGRES_DB_URL")
        if not self.db_url:
            # Default to the provided Neon database URL
            self.db_url = "postgresql://neondb_owner:npg_Td9jOSCDHrh1@ep-fragrant-snow-a8via4xi-pooler.eastus2.azure.neon.tech/employees?sslmode=require&channel_binding=require"
        
        self.model = ChatOpenAI(
            model="gpt-4o-mini",
            openai_api_key=os.getenv("OPENAPI_KEY")
        )
        
        # Database schema context
        self.db_context = self._build_database_context()
        print(self.db_context)
        
    def _get_connection(self):
        """Get a PostgreSQL connection"""
        return psycopg2.connect(self.db_url)
        
    def _build_database_context(self) -> str:
        """Build comprehensive database context by dynamically querying the database schema"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get all table names from the employees schema (Neon employees database)
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'employees' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = cursor.fetchall()
            
            context_parts = ["DATABASE SCHEMA FOR EMPLOYEES DATABASE (Neon Sample):\n"]
            
            # Build context for each table
            for table in tables:
                table_name = table[0]
                
                # Get table schema
                cursor.execute("""
                    SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, 
                           CASE WHEN pk.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END as is_primary_key
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT ku.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage ku 
                            ON tc.constraint_name = ku.constraint_name
                        WHERE tc.constraint_type = 'PRIMARY KEY' 
                        AND ku.table_name = %s
                        AND tc.table_schema = 'employees'
                    ) pk ON c.column_name = pk.column_name
                    WHERE c.table_name = %s AND c.table_schema = 'employees'
                    ORDER BY c.ordinal_position
                """, (table_name, table_name))
                columns = cursor.fetchall()
                
                # Get foreign key information
                cursor.execute("""
                    SELECT 
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_name = %s
                    AND tc.table_schema = 'employees'
                """, (table_name,))
                foreign_keys = cursor.fetchall()
                
                # Build table description
                context_parts.append(f"{len(context_parts)}. {table_name.upper()} TABLE:")
                
                for col in columns:
                    col_name, col_type, is_nullable, default_val, is_pk = col
                    
                    # Find foreign key info for this column
                    fk_info = ""
                    for fk in foreign_keys:
                        if fk[0] == col_name:  # Column name in foreign key
                            fk_info = f" (FOREIGN KEY -> {fk[1]}.{fk[2]})"
                            break
                    
                    # Build column description
                    col_desc = f"   - {col_name} ({col_type}"
                    if is_pk == 'YES':
                        col_desc += ", PRIMARY KEY"
                    if is_nullable == 'NO':
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
                cursor.execute("""
                    SELECT 
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_name = %s
                    AND tc.table_schema = 'employees'
                """, (table_name,))
                foreign_keys = cursor.fetchall()
                
                for fk in foreign_keys:
                    local_column = fk[0]
                    fk_table = fk[1]
                    fk_column = fk[2]
                    
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
                "- Use GROUP BY for grouped aggregations",
                "- Use PostgreSQL-specific functions like NOW(), CURRENT_DATE, etc.",
                "- All tables are in the 'employees' schema, so use 'employees.table_name' or set search_path",
                "- Use 'employees.employee', 'employees.department', 'employees.salary', etc."
            ])
            
            conn.close()
            
            return "\n".join(context_parts)
            
        except Exception as e:
            # Fallback to basic context if schema querying fails
            return f"""
DATABASE SCHEMA FOR EMPLOYEES DATABASE (Neon Sample):

Note: Unable to dynamically load schema due to error: {str(e)}
Using fallback context - consider checking database connection and permissions.

The employees database contains the following tables in the 'employees' schema:
- employees.employee - Employee information
- employees.department - Department information  
- employees.dept_emp - Employee-department assignments
- employees.dept_manager - Department managers
- employees.salary - Employee salary history
- employees.title - Employee job titles

Sample queries:
- SELECT * FROM employees.employee LIMIT 5;
- SELECT d.dept_name, AVG(s.amount) FROM employees.department d JOIN employees.dept_emp de ON d.id = de.department_id JOIN employees.salary s ON de.employee_id = s.employee_id GROUP BY d.dept_name;
- SELECT COUNT(*) FROM employees.employee;

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
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Execute query
            cursor.execute(query)
            
            # If the query returns rows, fetch them
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                data = []
                for row in results:
                    row_dict = {}
                    for key, value in row.items():
                        if hasattr(value, 'isoformat'):
                            row_dict[key] = value.isoformat()
                        else:
                            row_dict[key] = value
                    data.append(row_dict)
                conn.close()
                return {
                    "success": True,
                    "query": query,
                    "columns": columns,
                    "row_count": len(data),
                    "data": data
                }
            else:
                # DDL/DML: commit and return success
                conn.commit()
                conn.close()
                return {
                    "success": True,
                    "query": query,
                    "columns": [],
                    "row_count": 0,
                    "data": [],
                    "message": "Query executed successfully (no result set)"
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
You are a SQL query generator for the Employees Database (Neon sample database) using PostgreSQL. 

{self.db_context}

INSTRUCTIONS:
1. Generate ONLY valid PostgreSQL SQL queries
2. Use appropriate JOINs to get related data
3. Include proper WHERE clauses for filtering
4. Use ORDER BY for meaningful sorting
5. Limit results to reasonable amounts (use LIMIT 50 if not specified)
6. Return ONLY the SQL query, no explanations
7. Use PostgreSQL syntax (e.g., ILIKE instead of LIKE for case-insensitive matching)
8. Use proper PostgreSQL data types and functions
9. All tables are in the 'employees' schema, so prefix table names with 'employees.' or set search_path

EXAMPLE PROMPTS AND QUERIES:
- "Show all employees": SELECT * FROM employees.employee LIMIT 50
- "Employees in Sales department": SELECT e.*, d.dept_name FROM employees.employee e JOIN employees.dept_emp de ON e.id = de.employee_id JOIN employees.department d ON de.department_id = d.id WHERE d.dept_name = 'Sales'
- "Average salary by department": SELECT d.dept_name, AVG(s.amount) as avg_salary FROM employees.department d JOIN employees.dept_emp de ON d.id = de.department_id JOIN employees.salary s ON de.employee_id = s.employee_id WHERE s.to_date > CURRENT_DATE AND de.to_date > CURRENT_DATE GROUP BY d.dept_name
- "Current employees": SELECT * FROM employees.employee e JOIN employees.dept_emp de ON e.id = de.employee_id WHERE de.to_date > CURRENT_DATE
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
            "Find current employees with salary above 50000",
            "Show departments with their average salary",
            "Calculate average salary by department",
            "Find employees who are department managers",
            "Show employee titles and their counts",
            "List departments with the most employees",
            "Find employees with the highest salaries",
            "Show salary history for a specific employee",
            "Calculate total salary budget by department"
        ]

def create_sql_agent() -> SQLQueryExecutor:
    """Create a pre-configured SQL Query Executor"""
    return SQLQueryExecutor()

def interactive_sql_chat():
    """Run an interactive SQL query session"""
    print("🗄️  PostgreSQL Query Executor (OpenAI)")
    print("=" * 50)
    
    try:
        agent = create_sql_agent()
        print("✅ Connected to PostgreSQL database successfully")
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
        print("Make sure POSTGRES_DB_URL and OPENAPI_KEY are set in your .env file")

if __name__ == "__main__":
    # Check if required environment variables are available
    if not os.getenv("OPENAPI_KEY"):
        print("❌ Error: OPENAPI_KEY not found in environment variables")
        print("Please make sure your .env file contains: OPENAPI_KEY=your_api_key_here")
        exit(1)
    
    interactive_sql_chat()
