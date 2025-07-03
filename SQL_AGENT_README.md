# SQL Query Executor Agent

A powerful SQL query executor that uses OpenAI gpt-4o-mini to convert natural language prompts into SQL queries and execute them against the Employees Database (Neon sample database).

## Features

- 🤖 **AI-Powered Query Generation**: Convert natural language to SQL using OpenAI gpt-4o-mini
- 📊 **Database Context Awareness**: Full understanding of database schema and relationships
- 🔄 **Structured JSON Output**: Consistent, machine-readable response format
- 🛡️ **Error Handling**: Robust error handling for invalid queries
- 📝 **Query History**: Track generated queries and execution results
- 🎯 **Interactive Mode**: Command-line interface for easy testing

## Database Schema

The agent works with the Employees Database (Neon sample database) containing:

### Tables (in employees schema)
1. **employees.employee** - Employee information with personal details
2. **employees.department** - Department information with names and locations
3. **employees.dept_emp** - Employee-department assignments with date ranges
4. **employees.dept_manager** - Department manager assignments with date ranges
5. **employees.salary** - Employee salary history with date ranges
6. **employees.title** - Employee job titles with date ranges

### Key Relationships
- Employees are assigned to Departments through dept_emp (many-to-many with date ranges)
- Employees can be Department Managers through dept_manager (many-to-many with date ranges)
- Employees have Salary History through salary table (one-to-many with date ranges)
- Employees have Title History through title table (one-to-many with date ranges)
- All relationships include date ranges (from_date, to_date) for historical tracking

## Installation & Setup

1. **Environment Variables**: Ensure your `.env` file contains:
   ```
   OPENAPI_KEY=your_OPENAPI_KEY_here
   POSTGRES_DB_URL=postgresql://neondb_owner:npg_Td9jOSCDHrh1@ep-fragrant-snow-a8via4xi-pooler.eastus2.azure.neon.tech/employees?sslmode=require&channel_binding=require
   ```

2. **Database Setup**: Run the setup script to initialize the employees database:
   ```bash
   python setup_employees_db.py
   ```

3. **Dependencies**: Install required packages:
   ```bash
   pip install langchain-openai openai python-dotenv psycopg2-binary
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
- "Find current employees with salary above 50000"

### Complex Queries
- "Show employees with their departments and current salaries"
- "Calculate average salary by department"
- "Find employees who are department managers"
- "Show employee titles and their counts"

### Aggregation Queries
- "Calculate total salary by department"
- "Show departments with the most employees"
- "Find the highest paid employee"
- "Calculate average salary by job title"

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

The database contains the Neon employees sample data:
- 9 departments (Customer Service, Development, Finance, Human Resources, Marketing, Production, Quality Management, Research, Sales)
- 300,024 employees with comprehensive information
- Historical salary data with date ranges
- Job title history with date ranges
- Department assignment history with date ranges

## Limitations

- Currently supports PostgreSQL databases only
- Query results are limited to 50 rows by default
- Complex analytical queries may require optimization
- Generated queries may need manual review for complex business logic
- All tables are in the 'employees' schema and require proper schema prefixing

## Future Enhancements

- Support for other database types (PostgreSQL, MySQL)
- Query optimization suggestions
- Query performance analysis
- Natural language query explanations
- Query template library
- Advanced filtering and sorting options 