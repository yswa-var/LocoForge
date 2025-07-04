# 🚀 LocoForge: Advanced AI-Powered Database Orchestration System

> **A sophisticated hybrid database query orchestration system built with LangGraph, featuring intelligent query classification, multi-agent execution, seamless SQL/NoSQL integration, and professional data engineering support.**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![LangGraph](https://img.shields.io/badge/LangGraph-Latest-green.svg)](https://langchain-ai.github.io/langgraph/)
[![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4o--mini-orange.svg)](https://openai.com)
[![Architecture](https://img.shields.io/badge/Architecture-Hybrid%20Orchestrator-purple.svg)]()
[![Deployment](https://img.shields.io/badge/Deployment-Render%20Ready-blue.svg)](https://render.com)
[![Web Interface](https://img.shields.io/badge/Web%20Interface-Flask%20API-red.svg)](https://flask.palletsprojects.com/)

## 🎯 Project Overview
<img width="1438" alt="Screenshot 2025-06-27 at 1 30 14 PM" src="https://github.com/user-attachments/assets/6d4cb976-40f4-4152-b681-ae8580e9b21c" />

LocoForge is a cutting-edge **AI-powered database orchestration system** that intelligently routes and executes queries across multiple database types (SQL and NoSQL) using advanced graph-based workflows. The system leverages **LangGraph** for state management and **GPT-4o-mini** for intelligent query classification and decomposition.

### 🌟 Key Features

- **🤖 Intelligent Query Classification**: AI-powered domain and intent recognition with complexity assessment
- **🔄 Multi-Agent Orchestration**: Seamless SQL and NoSQL agent coordination
- **📊 Hybrid Query Processing**: Complex queries spanning multiple database types
- **🎯 Graph-Based Workflow**: Stateful execution with conditional routing
- **📈 Result Aggregation**: Intelligent combination of multi-source results
- **🔄 Context Management**: Persistent conversation history and state tracking
- **🔧 LangGraph Studio Integration**: Real-time workflow visualization and debugging
- **👨‍💼 Data Engineer Agent**: Professional handling of unclear, technical, and non-domain queries
- **🌐 Web Interface**: RESTful API with health checks and database statistics
- **🚀 Production Ready**: Docker support and Render deployment configuration
- **🛡️ Enhanced Error Handling**: Graceful degradation and comprehensive error recovery

## 🏗️ Architecture

### Core Components
![Editor _ Mermaid Chart-2025-06-27-083351](https://github.com/user-attachments/assets/8221623d-2f14-4bb8-86f5-feb5746d32bb)

### Enhanced Workflow Graph

The system implements a sophisticated **state machine** using LangGraph with the following nodes:

1. **`classify_query`** - AI-powered query domain, intent, and complexity classification
2. **`decompose_query`** - Complex query decomposition into sub-queries
3. **`route_to_agents`** - Intelligent routing decision making
4. **`sql_agent`** - SQL query execution (Employee Management)
5. **`nosql_agent`** - NoSQL query execution (Movie Database)
6. **`data_engineer`** - Professional handling of unclear/technical queries
7. **`aggregate_results`** - Multi-source result combination
8. **`update_context`** - Conversation state management
9. **`format_response`** - Final response formatting

<img width="1021" alt="Screenshot 2025-06-29 at 9 21 49 PM" src="https://github.com/user-attachments/assets/3bbfe0c3-c950-4429-b194-9c6dcbb91e7f" />

## 🛠️ Technical Implementation

### Enhanced State Management

```python
class OrchestratorState(TypedDict):
    messages: List[BaseMessage]           # Conversation history
    current_query: str                    # Current user query
    query_domain: QueryDomain            # Classified domain (EMPLOYEE/MOVIES/HYBRID/UNCLEAR/TECHNICAL)
    query_intent: QueryIntent            # Query intent (SELECT/ANALYZE/COMPARE/AGGREGATE/CLARIFY/EXPLAIN)
    query_complexity: QueryComplexity    # NEW: Complexity assessment (SIMPLE/MEDIUM/COMPLEX)
    sub_queries: Dict[str, str]          # Decomposed sub-queries
    sql_results: Optional[Dict[str, Any]] # SQL agent results
    nosql_results: Optional[Dict[str, Any]] # NoSQL agent results
    combined_results: Optional[Dict[str, Any]] # Aggregated results
    context_history: List[Dict[str, Any]] # Execution context
    execution_path: List[str]            # Workflow execution trace
    error_message: Optional[str]         # Error handling
    clarification_suggestions: Optional[List[str]] # NEW: Query refinement suggestions
    data_engineer_response: Optional[str] # NEW: Data engineer agent responses
```

### Enhanced Conditional Routing Logic

The system implements sophisticated routing decisions with new data engineer support:

```python
def route_decision(state: OrchestratorState) -> str:
    """Intelligent routing based on query domain and complexity"""
    domain = state["query_domain"]

    if domain == QueryDomain.EMPLOYEE:
        return "sql_only"
    elif domain == QueryDomain.MOVIES:
        return "nosql_only"
    elif domain == QueryDomain.HYBRID:
        return "both_agents"
    elif domain in [QueryDomain.UNCLEAR, QueryDomain.TECHNICAL]:
        return "data_engineer"  # NEW: Route unclear queries to Data Engineer
    else:
        return "error_handling"
```

### Data Engineer Agent

The new **Data Engineer Agent** provides professional handling for:

- **Ambiguous Queries**: "Show me everything", "What's the data?"
- **Non-Domain Queries**: "What's the weather like?", "Tell me a joke"
- **Technical Queries**: "SELECT * FROM employees", "Show schema"
- **Overly Complex Queries**: Performance-impacting queries

```python
class DataEngineerAgent:
    def analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze query and provide professional guidance"""
        
    def provide_clarification_suggestions(self, query: str, analysis: Dict[str, Any]) -> List[str]:
        """Generate specific clarification suggestions"""
        
    def handle_technical_query(self, query: str) -> Dict[str, Any]:
        """Handle SQL/NoSQL syntax and schema questions"""
        
    def handle_non_domain_query(self, query: str) -> Dict[str, Any]:
        """Handle queries outside system domain"""
```

### AI-Powered Query Classification

```python
def classify_intent(self, query: str) -> Tuple[QueryDomain, QueryIntent, QueryComplexity]:
    """Use GPT-4o-mini to classify query domain, intent, and complexity"""
    system_prompt = """
    You are an expert query classifier for a hybrid database system with:
    1. SQL Database: Employee management (employees, departments, projects, attendance)
    2. NoSQL Database: Movie database (movies, comments, users, theaters)

    Classify the query into:
    - DOMAIN: employee, movies, hybrid, unclear, technical
    - INTENT: select, analyze, compare, aggregate, clarify, explain
    - COMPLEXITY: simple, medium, complex
    """
    # LLM-based classification logic
```

## 🌐 Web Interface

### RESTful API Endpoints

The system now includes a complete web interface with the following endpoints:

```bash
# Health check
GET /health

# Database statistics
GET /api/database/stats

# Natural language query processing
POST /api/query
{
    "query": "Show me employee salaries and movie ratings"
}

# Direct SQL query execution
POST /api/database/query
{
    "sql": "SELECT COUNT(*) FROM employees WHERE department = 'Engineering'"
}
```

### Example API Usage

```bash
# Health check
curl https://your-app-name.onrender.com/health

# Natural language query
curl -X POST https://your-app-name.onrender.com/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "How many employees are in Engineering?"}'

# Database statistics
curl https://your-app-name.onrender.com/api/database/stats
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- MongoDB (for NoSQL operations)
- SQLite/PostgreSQL (for SQL operations)
- OpenAI API Key

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/LocoForge.git
cd LocoForge

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp env_template.txt .env
# Edit .env with your OpenAI API key and database configurations
```

### Environment Configuration

```bash
# .env file
OPENAI_API_KEY=your_openai_api_key_here
MONGO_DB=mongodb://localhost:27017/
SQL_DB=sqlite:///employee_management.db
ENVIRONMENT=development
```

### Quick Start

#### Using the Web Interface
```bash
# Start the Flask application
python app.py

# Access the API at http://localhost:5000
```

#### Using the LangGraph Workflow
```python
from my_agent.agent import graph
from my_agent.utils.state import OrchestratorState

# Initialize the workflow
workflow = graph

# Create a query
state = OrchestratorState(
    messages=[HumanMessage(content="Show me employee salaries and movie ratings")],
    current_query="Show me employee salaries and movie ratings"
)

# Execute the workflow
result = workflow.invoke(state)
print(result["combined_results"])
```

## 📊 Database Schemas

### SQL Database (Employee Management)

```sql
-- Employees table
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department TEXT,
    salary REAL,
    hire_date DATE,
    manager_id INTEGER
);

-- Departments table
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    budget REAL
);

-- Projects table
CREATE TABLE projects (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department_id INTEGER,
    start_date DATE,
    end_date DATE
);
```

### NoSQL Database (Movie Database)

```javascript
// Movies collection
{
  "_id": ObjectId,
  "title": "Movie Title",
  "year": 2020,
  "runtime": 120,
  "plot": "Movie description",
  "poster": "poster_url",
  "genres": ["Action", "Drama"],
  "cast": ["Actor 1", "Actor 2"],
  "directors": ["Director Name"],
  "imdb": {
    "rating": 8.5,
    "votes": 10000
  }
}

// Comments collection
{
  "_id": ObjectId,
  "movie_id": ObjectId,
  "name": "User Name",
  "email": "user@example.com",
  "text": "Great movie!",
  "date": ISODate("2024-01-15")
}
```

## 🧪 Testing

### Run Test Suite

```bash
# Test the orchestrator workflow
python test_orchestrator.py

# Test individual agents
python test_sql_agent.py
python test_nosql_agent.py

# Test cross-database queries
python test_cross_database_queries.py

# Test LangGraph Studio integration
python test_langgraph_studio.py

# Test edge cases and error handling
python test_edge_cases.py

# Test deployment configuration
python test_deployment.py
```

### Example Queries

```python
# Employee queries (SQL)
"Show me all employees in the Engineering department"
"What's the average salary by department?"
"Find employees hired in the last 6 months"

# Movie queries (NoSQL)
"Show me all action movies from 2020"
"What are the top-rated movies this year?"
"List movies with the most comments"

# Hybrid queries (Both databases)
"Show which employees watched action movies"
"Compare employee salaries with movie ratings"

# Unclear queries (Data Engineer)
"Show me everything"
"What's the weather like?"
"SELECT * FROM employees"
```

## 🚀 Deployment

### Render Deployment

The system includes complete Render deployment configuration:

```bash
# Deploy to Render using render.yaml
# The system automatically configures:
# - Flask web application
# - Gunicorn production server
# - Environment variable management
# - Health checks and monitoring
```

### Docker Deployment

```bash
# Build and run with Docker
docker build -t locoforge .
docker run -p 5000:5000 locoforge
```

### Environment Variables for Production

```bash
OPENAI_API_KEY=your_openai_api_key
GOOGLE_API_KEY=your_google_api_key
DATABASE_URL=sqlite:///employee_management.db
ENVIRONMENT=production
```

## 🔧 Advanced Features

### LangGraph Studio Integration

The system includes full LangGraph Studio support for real-time workflow visualization:

```bash
# Start LangGraph Studio
langgraph studio

# Access the interface at http://localhost:8123
```

### Enhanced Error Handling

The system includes comprehensive error handling:

- **Agent Initialization Failures**: Graceful degradation when agents are unavailable
- **Query Execution Errors**: Detailed error reporting and recovery
- **Network Connectivity**: Retry mechanisms for database connections
- **State Recovery**: Persistent state management across sessions
- **Data Engineer Fallback**: Professional responses for all query types

### Custom Agent Development

Extend the system with custom agents:

```python
class CustomAgent:
    def __init__(self):
        self.model = ChatOpenAI(model="gpt-4o-mini")

    def execute_query(self, query: str) -> Dict[str, Any]:
        # Custom query execution logic
        pass
```

## 📈 Performance & Scalability

### Optimization Strategies

- **Lazy Loading**: Agents initialized only when needed
- **Connection Pooling**: Efficient database connection management
- **Caching**: Query result caching for repeated requests
- **Async Processing**: Non-blocking query execution where possible
- **State Management**: Efficient state updates and context tracking

### Monitoring & Logging

```python
import logging

# Comprehensive logging throughout the workflow
logger = logging.getLogger(__name__)
logger.info("🔄 Initializing orchestrator...")
logger.info("✅ SQL agent initialized successfully")
logger.warning("⚠️ NoSQL agent not available")
logger.info("👨‍💼 Data Engineer Agent ready for unclear queries")
```

## 📚 Documentation

### Additional Guides

- **[DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)** - Complete deployment instructions
- **[EDGE_CASE_HANDLING_GUIDE.md](EDGE_CASE_HANDLING_GUIDE.md)** - Data Engineer Agent details
- **[SQL_AGENT_README.md](SQL_AGENT_README.md)** - SQL agent documentation
- **[NOSQL_AGENT_README.md](NOSQL_AGENT_README.md)** - NoSQL agent documentation
- **[MONGOENGINE_MIGRATION_README.md](MONGOENGINE_MIGRATION_README.md)** - Database migration guide

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **LangGraph Team** for the powerful workflow framework
- **OpenAI** for GPT-4o-mini capabilities
- **Render** for seamless deployment platform
- **MongoDB** for NoSQL database support
