# 🚀 LocoForge: Advanced AI-Powered Database Orchestration System

> **A sophisticated hybrid database query orchestration system built with LangGraph, featuring intelligent query classification, multi-agent execution, and seamless SQL/NoSQL integration.**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![LangGraph](https://img.shields.io/badge/LangGraph-Latest-green.svg)](https://langchain-ai.github.io/langgraph/)
[![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4o--mini-orange.svg)](https://openai.com)
[![Architecture](https://img.shields.io/badge/Architecture-Hybrid%20Orchestrator-purple.svg)]()

## 🎯 Project Overview
<img width="1438" alt="Screenshot 2025-06-27 at 1 30 14 PM" src="https://github.com/user-attachments/assets/6d4cb976-40f4-4152-b681-ae8580e9b21c" />

LocoForge is a cutting-edge **AI-powered database orchestration system** that intelligently routes and executes queries across multiple database types (SQL and NoSQL) using advanced graph-based workflows. The system leverages **LangGraph** for state management and **GPT-4o-mini** for intelligent query classification and decomposition.

### 🌟 Key Features

- **🤖 Intelligent Query Classification**: AI-powered domain and intent recognition
- **🔄 Multi-Agent Orchestration**: Seamless SQL and NoSQL agent coordination
- **📊 Hybrid Query Processing**: Complex queries spanning multiple database types
- **🎯 Graph-Based Workflow**: Stateful execution with conditional routing
- **📈 Result Aggregation**: Intelligent combination of multi-source results
- **🔄 Context Management**: Persistent conversation history and state tracking
- **🔧 LangGraph Studio Integration**: Real-time workflow visualization and debugging

## 🏗️ Architecture

### Core Components
![Editor _ Mermaid Chart-2025-06-27-083351](https://github.com/user-attachments/assets/8221623d-2f14-4bb8-86f5-feb5746d32bb)



### Workflow Graph

The system implements a sophisticated **state machine** using LangGraph with the following nodes:

1. **`classify_query`** - AI-powered query domain and intent classification
2. **`decompose_query`** - Complex query decomposition into sub-queries
3. **`route_to_agents`** - Intelligent routing decision making
4. **`sql_agent`** - SQL query execution (Employee Management)
5. **`nosql_agent`** - NoSQL query execution (Warehouse Management)
6. **`aggregate_results`** - Multi-source result combination
7. **`update_context`** - Conversation state management
8. **`format_response`** - Final response formatting

## 🛠️ Technical Implementation

### State Management

```python
class OrchestratorState(TypedDict):
    messages: List[BaseMessage]           # Conversation history
    current_query: str                    # Current user query
    query_domain: QueryDomain            # Classified domain (EMPLOYEE/WAREHOUSE/HYBRID)
    query_intent: QueryIntent            # Query intent (SELECT/ANALYZE/COMPARE/AGGREGATE)
    sub_queries: Dict[str, str]          # Decomposed sub-queries
    sql_results: Optional[Dict[str, Any]] # SQL agent results
    nosql_results: Optional[Dict[str, Any]] # NoSQL agent results
    combined_results: Optional[Dict[str, Any]] # Aggregated results
    context_history: List[Dict[str, Any]] # Execution context
    execution_path: List[str]            # Workflow execution trace
    error_message: Optional[str]         # Error handling
```

### Conditional Routing Logic

The system implements sophisticated routing decisions:

```python
def route_decision(state: OrchestratorState) -> str:
    """Intelligent routing based on query domain and complexity"""
    domain = state["query_domain"]

    if domain == QueryDomain.EMPLOYEE:
        return "sql_only"
    elif domain == QueryDomain.WAREHOUSE:
        return "nosql_only"
    elif domain == QueryDomain.HYBRID:
        return "both_agents"
    else:
        return "error_handling"
```

### AI-Powered Query Classification

```python
def classify_intent(self, query: str) -> Tuple[QueryDomain, QueryIntent]:
    """Use GPT-4o-mini to classify query domain and intent"""
    system_prompt = """
    You are an expert query classifier for a hybrid database system with:
    1. SQL Database: Employee management (employees, departments, projects, attendance)
    2. NoSQL Database: Grocery warehouse (products, inventory, orders, suppliers)

    Classify the query into:
    - DOMAIN: employee, warehouse, hybrid, unknown
    - INTENT: select, analyze, compare, aggregate
    """
    # LLM-based classification logic
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
cp .env.example .env
# Edit .env with your OpenAI API key and database configurations
```

### Environment Configuration

```bash
# .env file
OPENAI_API_KEY=your_openai_api_key_here
MONGO_DB=mongodb://localhost:27017/
SQL_DB=sqlite:///employee_management.db
```

### Quick Start

```python
from my_agent.agent import graph
from my_agent.utils.state import OrchestratorState

# Initialize the workflow
workflow = graph

# Create a query
state = OrchestratorState(
    messages=[HumanMessage(content="Show me employee salaries and warehouse inventory levels")],
    current_query="Show me employee salaries and warehouse inventory levels"
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

### NoSQL Database (Warehouse Management)

```javascript
// Products collection
{
  "_id": ObjectId,
  "name": "Product Name",
  "category": "Category",
  "price": 29.99,
  "supplier": "Supplier Name",
  "inventory": {
    "quantity": 150,
    "location": "A1-B2-C3",
    "reorder_level": 50
  }
}

// Orders collection
{
  "_id": ObjectId,
  "customer_id": "CUST001",
  "products": [
    {
      "product_id": ObjectId,
      "quantity": 5,
      "price": 29.99
    }
  ],
  "order_date": ISODate("2024-01-15"),
  "status": "pending"
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
```

### Example Queries

```python
# Employee queries (SQL)
"Show me all employees in the Engineering department"
"What's the average salary by department?"
"Find employees hired in the last 6 months"

# Warehouse queries (NoSQL)
"Show me all the product ids"
"What are the top-selling products this month?"
"list all the order data where customer id is 'CUST001'"

# Hybrid queries (Both databases)
"Show which employees placed orders for organic products"
"Show employees who work on multiple projects and placed orders for those projects"
```

## 🔧 Advanced Features

### LangGraph Studio Integration

The system includes full LangGraph Studio support for real-time workflow visualization:

```bash
# Start LangGraph Studio
langgraph studio

# Access the interface at http://localhost:8123
```

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

### Error Handling & Resilience

The system includes comprehensive error handling:

- **Agent Initialization Failures**: Graceful degradation when agents are unavailable
- **Query Execution Errors**: Detailed error reporting and recovery
- **Network Connectivity**: Retry mechanisms for database connections
- **State Recovery**: Persistent state management across sessions

## 📈 Performance & Scalability

### Optimization Strategies

- **Lazy Loading**: Agents initialized only when needed
- **Connection Pooling**: Efficient database connection management
- **Caching**: Query result caching for repeated requests
- **Async Processing**: Non-blocking query execution where possible

### Monitoring & Logging

```python
import logging

# Comprehensive logging throughout the workflow
logger = logging.getLogger(__name__)
logger.info("🔄 Initializing orchestrator...")
logger.info("✅ SQL agent initialized successfully")
logger.warning("⚠️ NoSQL agent not available")
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏆 Interview Highlights

### Technical Excellence

- **Advanced AI Integration**: Sophisticated use of GPT-4o-mini for query understanding
- **Graph-Based Architecture**: Modern workflow orchestration with LangGraph
- **Multi-Database Support**: Seamless SQL and NoSQL integration
- **State Management**: Complex stateful workflows with proper error handling

### System Design

- **Scalable Architecture**: Modular design supporting custom agent extensions
- **Production Ready**: Comprehensive error handling, logging, and monitoring
- **Developer Experience**: LangGraph Studio integration for debugging
- **Documentation**: Extensive documentation and examples

### Problem Solving

- **Complex Query Processing**: Intelligent decomposition of multi-domain queries
- **Result Aggregation**: Sophisticated combination of heterogeneous data sources
- **Context Management**: Persistent conversation state across sessions
- **Performance Optimization**: Efficient resource utilization and caching

---

**Built with ❤️ using LangGraph, OpenAI, and modern Python practices**
