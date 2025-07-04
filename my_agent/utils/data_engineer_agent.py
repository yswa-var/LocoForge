"""
Data Engineer Agent for Handling Unclear, Irrelevant, and Technical Queries
Provides professional guidance, query refinement, and context-aware responses
"""

import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from my_agent.utils.state import QueryDomain, QueryIntent, QueryComplexity
import os
from dotenv import load_dotenv

# Set up logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DataEngineerAgent:
    """Professional Data Engineer Agent for handling complex and unclear queries"""
    
    def __init__(self):
        """Initialize the Data Engineer Agent"""
        self.model = ChatOpenAI(
            model="gpt-4o-mini",
            api_key=os.getenv("OPENAPI_KEY") or os.getenv("OPENAI_API_KEY")
        )
        
        # Database context for professional responses
        self.database_context = {
            "sql_database": {
                "name": "Employees Database (Neon Sample)",
                "tables": ["employees.employee", "employees.department", "employees.dept_emp", "employees.dept_manager", "employees.salary", "employees.title"],
                "description": "PostgreSQL database containing comprehensive employee information, department data, salary history, and job titles",
                "key_entities": ["employees", "departments", "salaries", "titles"],
                "sample_queries": [
                    "Show all employees in the Sales department",
                    "Find current employees with salary above $50,000",
                    "List departments with their average salary",
                    "Show employee titles and their counts"
                ]
            },
            "nosql_database": {
                "name": "Sample Mflix Database",
                "collections": ["movies", "comments", "users", "sessions", "theaters", "embedded_movies"],
                "description": "NoSQL database containing movie information, user comments, ratings, and theater locations",
                "key_entities": ["movies", "comments", "users", "sessions", "theaters"],
                "sample_queries": [
                    "Find action movies with high ratings",
                    "Show movies from 2020",
                    "List movies with the most comments",
                    "Show top rated directors"
                ]
            }
        }
    
    def analyze_query(self, query: str) -> Dict[str, Any]:
        """
        Analyze a query to determine its nature and provide professional guidance
        
        Args:
            query: User query string
            
        Returns:
            Dictionary with analysis results and recommendations
        """
        system_prompt = self._get_analysis_prompt()
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Analyze this query: {query}")
        ]
        
        try:
            response = self.model.invoke(messages)
            analysis = json.loads(response.content)
            return analysis
        except Exception as e:
            logger.error(f"Error analyzing query: {e}")
            return self._get_default_analysis(query)
    
    def provide_clarification_suggestions(self, query: str, analysis: Dict[str, Any]) -> List[str]:
        """
        Generate clarification suggestions for unclear queries
        
        Args:
            query: Original query
            analysis: Query analysis results
            
        Returns:
            List of clarification suggestions
        """
        if analysis.get("is_clear", True):
            return []
        
        system_prompt = self._get_clarification_prompt()
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Query: {query}\nAnalysis: {json.dumps(analysis)}")
        ]
        
        try:
            response = self.model.invoke(messages)
            suggestions = json.loads(response.content)
            return suggestions.get("suggestions", [])
        except Exception as e:
            logger.error(f"Error generating suggestions: {e}")
            return self._get_default_suggestions(query)
    
    def handle_technical_query(self, query: str) -> Dict[str, Any]:
        """
        Handle technical queries like schema requests, database structure questions
        
        Args:
            query: Technical query
            
        Returns:
            Professional response with technical details
        """
        system_prompt = self._get_technical_prompt()
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Technical query: {query}")
        ]
        
        try:
            response = self.model.invoke(messages)
            return {
                "success": True,
                "response": response.content,
                "query_type": "technical",
                "original_query": query
            }
        except Exception as e:
            logger.error(f"Error handling technical query: {e}")
            return {
                "success": False,
                "error": str(e),
                "query_type": "technical",
                "original_query": query
            }
    
    def handle_non_domain_query(self, query: str) -> Dict[str, Any]:
        """
        Handle queries that are outside the system's domain
        
        Args:
            query: Non-domain query
            
        Returns:
            Professional response explaining system capabilities
        """
        system_prompt = self._get_non_domain_prompt()
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Non-domain query: {query}")
        ]
        
        try:
            response = self.model.invoke(messages)
            return {
                "success": True,
                "response": response.content,
                "query_type": "non_domain",
                "original_query": query
            }
        except Exception as e:
            logger.error(f"Error handling non-domain query: {e}")
            return {
                "success": False,
                "error": str(e),
                "query_type": "non_domain",
                "original_query": query
            }
    
    def handle_ambiguous_query(self, query: str, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle ambiguous queries by providing database context and guidance
        
        Args:
            query: Ambiguous query
            analysis: Query analysis results
            
        Returns:
            Professional response with database context and guidance
        """
        system_prompt = self._get_ambiguous_prompt()
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"Ambiguous query: {query}\nAnalysis: {json.dumps(analysis)}")
        ]
        
        try:
            response = self.model.invoke(messages)
            return {
                "success": True,
                "response": response.content,
                "query_type": "ambiguous",
                "original_query": query
            }
        except Exception as e:
            logger.error(f"Error handling ambiguous query: {e}")
            return {
                "success": False,
                "error": str(e),
                "query_type": "ambiguous",
                "original_query": query
            }
    
    def handle_sql_query_without_agent(self, query: str) -> Dict[str, Any]:
        """
        Handle SQL-related queries when SQL agent is not available
        
        Args:
            query: SQL-related query
            
        Returns:
            Professional response with database context and guidance
        """
        # First, diagnose the actual issue
        diagnosis = self._diagnose_sql_agent_issue()
        
        system_prompt = self._get_sql_guidance_prompt()
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=f"SQL-related query: {query}\n\nDiagnosis: {diagnosis}")
        ]
        
        try:
            response = self.model.invoke(messages)
            return {
                "success": True,
                "response": response.content,
                "query_type": "sql_guidance",
                "original_query": query,
                "execution_result": {
                    "success": False,
                    "error": diagnosis,
                    "row_count": 0,
                    "data": []
                }
            }
        except Exception as e:
            logger.error(f"Error handling SQL query without agent: {e}")
            return {
                "success": False,
                "error": str(e),
                "query_type": "sql_guidance",
                "original_query": query,
                "execution_result": {
                    "success": False,
                    "error": str(e),
                    "row_count": 0,
                    "data": []
                }
            }
    
    def _diagnose_sql_agent_issue(self) -> str:
        """
        Diagnose why the SQL agent is not working
        
        Returns:
            String describing the root cause
        """
        issues = []
        
        # Check if psycopg2 is available
        try:
            import psycopg2
            issues.append("✅ psycopg2 is available")
        except ImportError:
            issues.append("❌ psycopg2 is not available - install with: pip install psycopg2-binary")
        
        # Check if SQL agent can be imported
        try:
            from my_agent.utils.sql_agent import SQLQueryExecutor
            issues.append("✅ SQL agent module can be imported")
        except ImportError as e:
            issues.append(f"❌ SQL agent module import failed: {e}")
        
        # Check environment variables
        import os
        openai_key = os.getenv("OPENAPI_KEY") or os.getenv("OPENAI_API_KEY")
        postgres_url = os.getenv("POSTGRES_DB_URL")
        
        if openai_key:
            issues.append("✅ OpenAI API key is set")
        else:
            issues.append("❌ OpenAI API key is not set (OPENAPI_KEY or OPENAI_API_KEY)")
        
        if postgres_url:
            issues.append("✅ PostgreSQL URL is set")
        else:
            issues.append("❌ PostgreSQL URL is not set (POSTGRES_DB_URL)")
        
        # Try to initialize SQL agent to see what fails
        if all(["✅" in issue for issue in issues[:4]]):  # If all basic checks pass
            try:
                agent = SQLQueryExecutor()
                issues.append("✅ SQL agent initialized successfully")
                return "SQL agent should be working - this might be a LangGraph Studio environment issue"
            except Exception as e:
                issues.append(f"❌ SQL agent initialization failed: {e}")
        
        # Check if we're in LangGraph Studio context
        try:
            import langgraph
            issues.append("✅ LangGraph is available")
        except ImportError:
            issues.append("❌ LangGraph is not available")
        
        return "\n".join(issues)
    
    def _get_analysis_prompt(self) -> str:
        """Get the system prompt for query analysis"""
        return f"""
You are a professional Data Engineer analyzing queries for a hybrid database system.

DATABASE CONTEXT:
{json.dumps(self.database_context, indent=2)}

ANALYSIS TASK:
Analyze the query and return a JSON object with:
- "is_clear": boolean (true if query is clear and actionable)
- "query_type": string (one of: clear, ambiguous, non_domain, technical)
- "domain_relevance": string (employee, movies, hybrid, none)
- "complexity_level": string (simple, medium, complex)
- "confidence": float (0.0 to 1.0)
- "issues": list of strings (specific issues found)
- "suggested_domain": string (best domain match if unclear)

EXAMPLES:
- "Show me everything" → {{"is_clear": false, "query_type": "ambiguous", "domain_relevance": "none", "complexity_level": "simple", "confidence": 0.1, "issues": ["Too vague", "No specific domain"], "suggested_domain": "none"}}
- "What's the weather like?" → {{"is_clear": true, "query_type": "non_domain", "domain_relevance": "none", "complexity_level": "simple", "confidence": 1.0, "issues": ["Outside system scope"], "suggested_domain": "none"}}
- "Find employees with perfect attendance who placed orders over $100" → {{"is_clear": true, "query_type": "clear", "domain_relevance": "hybrid", "complexity_level": "medium", "confidence": 0.9, "issues": [], "suggested_domain": "hybrid"}}
- "Show all employees in IT department" → {{"is_clear": true, "query_type": "clear", "domain_relevance": "employee", "complexity_level": "simple", "confidence": 0.9, "issues": [], "suggested_domain": "employee"}}

IMPORTANT RULES:
- Queries that combine employee data (attendance, departments, projects) with movie data (movies, comments, ratings) should be classified as "hybrid" domain_relevance and "clear" query_type
- Only queries completely outside the system scope (weather, cooking, etc.) should be "non_domain"
- Hybrid queries are valid and should be processed by the system
- Direct SQL/NoSQL queries should be classified as "clear" with appropriate domain_relevance
- "Technical" query_type should only be used for questions about database structure, schema, or system capabilities, NOT for actual SQL/NoSQL commands

Return ONLY the JSON object.
"""
    
    def _get_clarification_prompt(self) -> str:
        """Get the system prompt for generating clarification suggestions"""
        return f"""
You are a professional Data Engineer helping users refine unclear queries.

DATABASE CONTEXT:
{json.dumps(self.database_context, indent=2)}

TASK:
Generate 3-5 specific clarification suggestions for the unclear query.
Each suggestion should be a complete, actionable query.

Return JSON with "suggestions" field containing a list of strings.

EXAMPLES:
For "Show me everything":
{{
      "suggestions": [
        "Show all employees in the company",
        "Find action movies with high ratings",
        "Display all departments and their budgets",
        "Show movies from 2020",
        "List all active projects and their status"
      ]
}}

For "What's the data?":
{{
      "suggestions": [
        "Show a summary of employee data",
        "Display movie database overview",
        "List all available data categories",
        "Show sample data from each database"
      ]
}}

Return ONLY the JSON object.
"""
    
    def _get_technical_prompt(self) -> str:
        """Get the system prompt for handling technical queries"""
        return f"""
You are a professional Data Engineer providing technical guidance about database systems.

DATABASE CONTEXT:
{json.dumps(self.database_context, indent=2)}

TASK:
Provide a professional, helpful response to technical queries about the database system.
Include relevant technical details, schema information, and usage examples.

RESPONSE FORMAT:
- Be professional and technical
- Include specific database/table/collection names
- Provide example queries when relevant
- Explain limitations and capabilities
- Suggest best practices

EXAMPLES:
- For schema requests: Explain table structures and relationships
- For query language questions: Provide syntax examples
- For performance questions: Explain optimization strategies
- For data access questions: Show proper query patterns

Be helpful, accurate, and professional.
"""
    
    def _get_non_domain_prompt(self) -> str:
        """Get the system prompt for handling non-domain queries"""
        return f"""
You are a professional Data Engineer responding to queries outside your system's scope.

DATABASE CONTEXT:
{json.dumps(self.database_context, indent=2)}

TASK:
Politely explain that the query is outside the system's domain and provide:
1. Clear explanation of what the system can do
2. Examples of supported queries
3. Professional tone with helpful suggestions

RESPONSE GUIDELINES:
- Be polite and professional
- Clearly explain system capabilities
- Provide relevant examples
- Suggest alternative approaches if possible
- Maintain helpful tone

EXAMPLE RESPONSE:
"I understand you're asking about [topic]. This system is specifically designed for [employee management and warehouse operations]. 

Here are some examples of what I can help you with:
- Employee data: salaries, departments, attendance, projects
- Movie data: movies, ratings, comments, theaters
- Cross-domain analysis: employee movie preferences, department entertainment patterns

Would you like to explore any of these areas instead?"

Be helpful and professional while clearly defining system boundaries.
"""
    
    def _get_ambiguous_prompt(self) -> str:
        """Get the system prompt for handling ambiguous queries"""
        return f"""
You are a professional Data Engineer helping users with unclear database queries.

DATABASE CONTEXT:
{json.dumps(self.database_context, indent=2)}

TASK:
Provide a helpful, database-focused response to ambiguous queries. Instead of just saying "I can't process this", give users useful information about what data is available and how they can query it.

RESPONSE GUIDELINES:
1. Acknowledge the query is unclear but be helpful
2. Explain what data is available in the system
3. Provide specific examples of what they can ask about
4. Show the database structure and available tables/collections
5. Give concrete query examples they can use
6. Be professional and encouraging

EXAMPLE RESPONSES:
For "what the sql database schema?":
"I understand you want to know about the database structure. Let me show you what's available in our Employee Management System:

**SQL Database (Employee Management System):**
- **employees** table: id, name, email, salary, department_id, hire_date
- **departments** table: id, name, budget, manager_id
- **projects** table: id, name, description, status, start_date, end_date
- **attendance** table: id, employee_id, date, check_in, check_out

**NoSQL Database (Sample Mflix - Movies):**
- **movies** collection: title, year, genres, cast, directors, ratings
- **comments** collection: user comments on movies
- **users** collection: user information and accounts
- **sessions** collection: user session data
- **theaters** collection: movie theater locations

You can ask specific questions like:
- 'Show all employees in the IT department'
- 'Find action movies with high ratings'
- 'Find employees with salary above $50,000'
- 'Show movies from 2020'

What specific information would you like to see?"

Be helpful, informative, and guide users toward specific, actionable queries.
"""
    
    def _get_sql_guidance_prompt(self) -> str:
        """Get the system prompt for handling SQL queries when SQL agent is not available"""
        return f"""
You are a professional Data Engineer providing guidance for SQL queries when the SQL agent is temporarily unavailable.

DATABASE CONTEXT:
{json.dumps(self.database_context, indent=2)}

SITUATION:
The SQL agent is currently unavailable. I can provide you with detailed information about the database structure and help you understand what queries would work.

SQL DATABASE SCHEMA (Employee Management System):
The employees database contains the following tables in the 'employees' schema:
- **employees.employee**: Employee information (id, first_name, last_name, birth_date, gender, hire_date)
- **employees.department**: Department information (id, dept_name, location)
- **employees.dept_emp**: Employee-department assignments (employee_id, department_id, from_date, to_date)
- **employees.dept_manager**: Department managers (employee_id, department_id, from_date, to_date)
- **employees.salary**: Employee salary history (employee_id, amount, from_date, to_date)
- **employees.title**: Employee job titles (employee_id, title, from_date, to_date)

TASK:
Provide a professional response that:
1. Acknowledges the SQL agent is temporarily unavailable
2. Explains the database structure and available data
3. Shows what the query would return if the SQL agent were available
4. Provides alternative ways to get similar information
5. Uses the diagnosis information to provide specific troubleshooting steps

IMPORTANT: Use the diagnosis information provided to give specific, actionable troubleshooting steps. Don't assume it's just a missing dependency - check what the diagnosis actually says.

RESPONSE FORMAT:
- Be professional and helpful
- Explain the database structure clearly
- Show what data would be returned
- Provide alternative approaches
- Include troubleshooting steps

EXAMPLE:
"I understand you want to query the employee database. While the SQL agent is temporarily unavailable, I can help you understand what data is available and what your query would return.

Your query would access the Employee Management System, which contains comprehensive employee data including:
- Employee personal information (names, birth dates, hire dates)
- Department assignments and management roles
- Salary history and job titles
- Current and historical employment data

Based on the diagnosis, here are the specific issues and solutions:
[Use the actual diagnosis information to provide targeted troubleshooting steps]

In the meantime, you can ask me about the database structure, available tables, and what kind of queries would work once the SQL agent is available."

Be helpful, informative, and maintain a professional tone while explaining the technical situation.
"""
    
    def _get_default_analysis(self, query: str) -> Dict[str, Any]:
        """Get default analysis when LLM fails"""
        return {
            "is_clear": False,
            "query_type": "ambiguous",
            "domain_relevance": "none",
            "complexity_level": "simple",
            "confidence": 0.0,
            "issues": ["Unable to analyze query"],
            "suggested_domain": "none"
        }
    
    def _get_default_suggestions(self, query: str) -> List[str]:
        """Get default suggestions when LLM fails"""
        return [
            "Show all employees in the company",
            "Find action movies with high ratings",
            "Display all departments and their budgets",
            "Show movies from 2020"
        ] 