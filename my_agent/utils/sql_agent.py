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
        
    def _build_database_context(self) -> str:
        """Build comprehensive database context including schema and relationships"""
        context = """
DATABASE SCHEMA FOR EMPLOYEE MANAGEMENT SYSTEM:

1. DEPARTMENTS TABLE:
   - department_id (INTEGER, PRIMARY KEY, AUTOINCREMENT)
   - department_name (TEXT, NOT NULL, UNIQUE)
   - location (TEXT, NOT NULL)
   - budget (REAL, DEFAULT 0.0)
   - created_date (DATE, DEFAULT CURRENT_DATE)

2. EMPLOYEES TABLE:
   - employee_id (INTEGER, PRIMARY KEY, AUTOINCREMENT)
   - first_name (TEXT, NOT NULL)
   - last_name (TEXT, NOT NULL)
   - email (TEXT, UNIQUE, NOT NULL)
   - phone (TEXT)
   - hire_date (DATE, NOT NULL)
   - salary (REAL, NOT NULL)
   - department_id (INTEGER, FOREIGN KEY -> departments.department_id)
   - manager_id (INTEGER, FOREIGN KEY -> employees.employee_id)
   - position (TEXT, NOT NULL)
   - status (TEXT, DEFAULT 'active')

3. PROJECTS TABLE:
   - project_id (INTEGER, PRIMARY KEY, AUTOINCREMENT)
   - project_name (TEXT, NOT NULL)
   - description (TEXT)
   - start_date (DATE, NOT NULL)
   - end_date (DATE)
   - budget (REAL, DEFAULT 0.0)
   - status (TEXT, DEFAULT 'active')
   - department_id (INTEGER, FOREIGN KEY -> departments.department_id)
   - project_manager_id (INTEGER, FOREIGN KEY -> employees.employee_id)

4. EMPLOYEE_PROJECTS TABLE (Junction table):
   - assignment_id (INTEGER, PRIMARY KEY, AUTOINCREMENT)
   - employee_id (INTEGER, NOT NULL, FOREIGN KEY -> employees.employee_id)
   - project_id (INTEGER, NOT NULL, FOREIGN KEY -> projects.project_id)
   - role (TEXT, NOT NULL)
   - hours_allocated (INTEGER, DEFAULT 0)
   - start_date (DATE, NOT NULL)
   - end_date (DATE)
   - UNIQUE(employee_id, project_id)

5. ATTENDANCE TABLE:
   - attendance_id (INTEGER, PRIMARY KEY, AUTOINCREMENT)
   - employee_id (INTEGER, NOT NULL, FOREIGN KEY -> employees.employee_id)
   - date (DATE, NOT NULL)
   - check_in_time (TIME)
   - check_out_time (TIME)
   - hours_worked (REAL, DEFAULT 0.0)
   - status (TEXT, DEFAULT 'present')
   - UNIQUE(employee_id, date)

RELATIONSHIPS:
- Employees belong to Departments (many-to-one)
- Employees can have Managers (self-referencing, many-to-one)
- Projects belong to Departments (many-to-one)
- Projects have Project Managers (many-to-one with employees)
- Employees can work on multiple Projects (many-to-many via employee_projects)
- Employees have Attendance records (one-to-many)

COMMON QUERY PATTERNS:
- JOIN employees with departments to get department info
- JOIN employees with projects via employee_projects
- Self-join employees for manager-subordinate relationships
- Aggregate functions for salary, budget, hours analysis
- Date-based queries for attendance and project timelines
"""
        return context
    
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
