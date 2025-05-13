# Multi-Database Query Orchestrator

A sophisticated system for natural language interaction with multiple databases (SQL, NoSQL, and Google Drive) using LangGraph and LangChain. This system provides a unified interface for querying different database types through natural language prompts.

## Architecture

The system implements a three-layer architecture for robust query processing:

### 1. Supervisor Layer

- Analyzes user queries and breaks them down into structured tasks
- Determines which database(s) need to be queried
- Generates task definitions with priorities and dependencies
- Maintains context awareness across multiple queries

### 2. Task Manager Layer

- Routes tasks to appropriate database agents
- Manages task execution order based on dependencies
- Handles task scheduling and parallel execution
- Aggregates results from different database agents

### 3. Error Handler Layer

- Validates task results and database responses
- Implements retry logic with maximum recursion (3 attempts)
- Performs schema validation for task feasibility
- Requests task refinement from supervisor when needed

## System Components

### Database Agents

1. **SQL Agent**

   - Handles SQLite database operations
   - Generates and executes SQL queries
   - Maintains connection pooling
   - Schema-aware query generation

2. **NoSQL Agent**

   - Manages MongoDB operations
   - Handles document-based queries
   - Supports complex aggregations
   - Schema inference and validation

3. **Google Drive Agent** (Planned)
   - Will handle file operations
   - Document search and retrieval
   - File metadata management

### Core Components

- `supervisor_node.py`: Implements the supervisor layer logic
- `task_manager.py`: Manages task routing and execution
- `error_handler.py`: Implements error handling and validation
- `graph.py`: Defines the LangGraph workflow
- `state.py`: Manages system state and context

## Setup

### Prerequisites

- Python 3.9+
- MongoDB
- SQLite
- Google Drive API credentials (for future implementation)

### Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/multi-db-query-orchestrator.git
cd multi-db-query-orchestrator
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Set up environment variables:

```bash
cp .env.example .env
```

Edit `.env` with your configuration:

```
OPENAI_API_KEY=your_openai_api_key
MONGODB_URI=mongodb://localhost:27017/
GOOGLE_DRIVE_CREDENTIALS=path_to_credentials.json
```

### Database Setup

1. **SQLite Setup**

   - The system uses SQLite by default
   - Database file location: `src/agent/sales.db`
   - Run initial schema setup:

   ```bash
   python scripts/setup_sqlite.py
   ```

2. **MongoDB Setup**
   - Ensure MongoDB is running locally or update connection string
   - Initialize collections:
   ```bash
   python scripts/setup_mongodb.py
   ```

## Usage

### Starting the System

1. Start the LangGraph Studio:

```bash
langgraph-studio
```

2. Open the project in LangGraph Studio:

```bash
langgraph-studio open .
```

### Example Queries

1. **Simple SQL Query**

```
"Show me all customers who made purchases in the last month"
```

2. **NoSQL Query**

```
"Find all orders with total value greater than $1000"
```

3. **Cross-Database Query**

```
"Get customer information and their recent order history"
```

### Query Flow

1. User submits a natural language query
2. Supervisor layer analyzes and breaks down the query
3. Task Manager routes tasks to appropriate agents
4. Error Handler validates results
5. System returns aggregated response

## Development

### Project Structure

```
multi-db-query-orchestrator/
├── src/
│   ├── agent/
│   │   ├── nodes.py
│   │   ├── sql_agent.py
│   │   ├── no_sql_agent.py
│   │   ├── task_manager.py
│   │   ├── error_handler.py
│   │   └── graph.py
│   ├── utils/
│   │   ├── logger.py
│   │   └── config.py
│   └── tests/
├── scripts/
│   ├── setup_sqlite.py
│   └── setup_mongodb.py
├── requirements.txt
├── .env.example
└── README.md
```

### Adding New Features

1. **New Database Agent**

   - Create new agent class in `src/agent/`
   - Implement required interface methods
   - Add to task manager routing
   - Update schema validation

2. **Custom Task Types**
   - Extend supervisor task generation
   - Add new task handlers
   - Update error handling logic

### Testing

Run tests:

```bash
pytest src/tests/
```

## Error Handling

The system implements comprehensive error handling:

1. **Task Validation**

   - Schema compatibility check
   - Query syntax validation
   - Resource availability verification

2. **Retry Logic**

   - Maximum 3 retry attempts
   - Exponential backoff
   - Task refinement when needed

3. **Error Recovery**
   - Graceful degradation
   - Partial results handling
   - User-friendly error messages

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License - see LICENSE file for details

## Support

For issues and feature requests, please use the GitHub issue tracker.

## Acknowledgments

- LangGraph for the workflow framework
- LangChain for the LLM integration
- MongoDB and SQLite communities
