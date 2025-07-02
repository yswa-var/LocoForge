# SQL Query Executor Agent

A powerful SQL query executor that uses OpenAI gpt-4o-mini to convert natural language prompts into SQL queries and execute them against the Employee Management database.

## Features

- 🤖 **AI-Powered Query Generation**: Convert natural language to SQL using OpenAI gpt-4o-mini
- 📊 **Database Context Awareness**: Full understanding of database schema and relationships
- 🔄 **Structured JSON Output**: Consistent, machine-readable response format
- 🛡️ **Error Handling**: Robust error handling for invalid queries
- 📝 **Query History**: Track generated queries and execution results
- 🎯 **Interactive Mode**: Command-line interface for easy testing

## Database Schema

The agent works with an Employee Management System containing:

### Tables
1. **departments** - Company departments with budgets and locations
2. **employees** - Employee information with department and manager relationships
3. **projects** - Project details with department and manager assignments
4. **employee_projects** - Many-to-many relationship between employees and projects
5. **attendance** - Employee attendance and time tracking

### Key Relationships
- Employees belong to Departments (many-to-one)
- Employees can have Managers (self-referencing)
- Projects belong to Departments (many-to-one)
- Employees can work on multiple Projects (many-to-many)
- Employees have Attendance records (one-to-many)

## Installation & Setup

1. **Environment Variables**: Ensure your `.env` file contains:
   ```
   OPENAI_API_KEY=your_openai_api_key_here
   SQL_DB=/Users/apple/lgstudioSetup/LocoForge/employee_management.db
   ```

2. **Dependencies**: Install required packages:
   ```bash
   pip install langchain-openai openai python-dotenv
   ```

## Usage

### Interactive Mode

Run the interactive SQL chat:
```bash
python my_agent/utils/sql_agent.py
```

### Programmatic Usage

```python
from my_agent.utils.sql_agent import SQLQueryExecutor

# Initialize the agent
agent = SQLQueryExecutor()

# Generate and execute a query
result = agent.generate_and_execute_query("Show all employees with their department names")

# Access the results
print(f"Generated SQL: {result['generated_sql']}")
print(f"Rows returned: {result['execution_result']['row_count']}")
print(f"Data: {result['execution_result']['data']}")
```

### Direct SQL Execution

```python
# Execute a raw SQL query
result = agent.execute_query("SELECT COUNT(*) FROM employees")
print(f"Total employees: {result['data'][0]['COUNT(*)']}")
```

## Response Format

The agent returns structured JSON responses:

```json
{
  "prompt": "Show all employees with their department names",
  "generated_sql": "SELECT e.*, d.department_name FROM employees e JOIN departments d ON e.department_id = d.department_id",
  "execution_result": {
    "success": true,
    "query": "SELECT e.*, d.department_name FROM employees e JOIN departments d ON e.department_id = d.department_id",
    "columns": ["employee_id", "first_name", "last_name", "email", "phone", "hire_date", "salary", "department_id", "manager_id", "position", "status", "department_name"],
    "row_count": 16,
    "data": [
      {
        "employee_id": 1,
        "first_name": "John",
        "last_name": "Smith",
        "email": "john.smith@company.com",
        "phone": "555-0101",
        "hire_date": "2020-01-15",
        "salary": 85000.0,
        "department_id": 1,
        "manager_id": null,
        "position": "Senior Software Engineer",
        "status": "active",
        "department_name": "Engineering"
      }
    ]
  },
  "timestamp": "2025-06-27T01:00:26.522058"
}
```

## Example Queries

### Simple Queries
- "Show all employees"
- "List all departments"
- "Find employees with salary above 50000"

### Complex Queries
- "Show employees with their managers and departments"
- "Calculate average salary by department"
- "Find employees working on multiple projects"
- "Show project managers and their projects"

### Aggregation Queries
- "Calculate total salary by department"
- "Show departments with their total budget"
- "Find the highest paid employee"
- "Calculate total hours worked by employee"

## Testing

Run the test script to see the agent in action:
```bash
python test_sql_agent.py
```

Run the example usage script:
```bash
python example_sql_usage.py
```

## Error Handling

The agent handles various error scenarios:
- Invalid SQL syntax
- Missing tables or columns
- Database connection issues
- API key authentication problems

Errors are returned in the response structure:
```json
{
  "execution_result": {
    "success": false,
    "error": "near \"ite\": syntax error",
    "data": []
  }
}
```

## Sample Data

The database contains sample employee management data:
- 6 departments (Engineering, Marketing, Sales, HR, Finance, Operations)
- 16 employees with various roles and salaries
- Multiple projects with employee assignments
- Attendance records for time tracking

## Limitations

- Currently supports SQLite databases only
- Query results are limited to 50 rows by default
- Complex analytical queries may require optimization
- Generated queries may need manual review for complex business logic

## Future Enhancements

- Support for other database types (PostgreSQL, MySQL)
- Query optimization suggestions
- Query performance analysis
- Natural language query explanations
- Query template library
- Advanced filtering and sorting options 