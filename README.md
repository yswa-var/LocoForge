# Multi-Agent Database System with LangGraph

![Screenshot 2025-05-08 at 2 13 45 PM](https://github.com/user-attachments/assets/7dbc007a-092b-444f-9b51-abb19ed55225)


## Features

- Natural language processing of database queries
- Intelligent routing between SQL, NoSQL, and Google Drive operations
- Schema-aware database operations
- Comprehensive error handling and logging
- Support for complex database operations
- Extensible architecture for adding new agents

## Prerequisites

- Python 3.9+
- MongoDB (for NoSQL operations)
- SQLite (for SQL operations)
- Google Drive API credentials (for Drive operations)
- LangGraph Studio installed

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd langgraph_as
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

5. Configure your `.env` file with the following variables:
```env
# LLM Configuration
OPENAI_API_KEY=your_openai_api_key
# Google Drive Configuration
GOOGLE_DRIVE_CREDENTIALS_FILE=path/to/credentials.json
```

## Setting up LangGraph Studio

1. Install LangGraph Studio:
```bash
pip install langgraph-studio
```

2. Start LangGraph Studio:
```bash
langgraph-studio
```

3. Open your browser and navigate to `http://localhost:3000`

4. Import the project:
   - Click on "Import Project"
   - Select the project directory
   - The system will automatically detect the graph configuration

## Using the System

1. **Starting a New Session**:
   - Click the "+" button in LangGraph Studio to start a new session
   - The system will initialize with the supervisor node

2. **Making Database Queries**:
   - Type your natural language query in the input box
   - The supervisor will analyze the query and route it to the appropriate agent
   - Results will be displayed in the chat interface

3. **Example Queries**:
   ```
   "Show me all users in the SQL database"
   "Find documents in MongoDB where status is active"
   "List all files in my Google Drive"
   ```

## Project Structure

```
src/
├── agent/
│   ├── graph.py          # Main graph definition
│   ├── nodes.py          # Node implementations
│   ├── state.py          # State management
│   ├── configuration.py  # System configuration
│   ├── sql_agent.py      # SQL database operations
│   ├── no_sql_agent.py   # MongoDB operations
│   └── drive_agent.py    # Google Drive operations
├── tests/                # Test files
└── static/              # Static assets
```

## Customization

1. **Adding New Agents**:
   - Create a new agent class in the `agent` directory
   - Add the agent node to `nodes.py`
   - Update the graph in `graph.py` to include the new agent

2. **Modifying the Supervisor**:
   - Edit the routing logic in `nodes.py`
   - Update the confidence thresholds and routing rules

3. **Extending Database Support**:
   - Add new database connection handlers
   - Implement query translation for the new database type

## Development

- Use LangGraph Studio's debugging features to:
  - Edit past state
  - Rerun from specific nodes
  - Monitor agent interactions
  - Analyze performance

- The system supports hot reloading for local development

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and feature requests, please create an issue in the repository.
