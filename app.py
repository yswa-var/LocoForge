import os
import sqlite3
from flask import Flask, jsonify, request
from datetime import datetime
import logging

# Import your existing modules
from my_agent.agent import create_agent
from sql_db_ops.sql_db_init import create_employee_management_db

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables
db_path = None
agent = None

def initialize_database():
    """Initialize the SQLite database on startup"""
    global db_path
    try:
        # Create database in the current directory (Render's ephemeral storage)
        db_path = os.path.join(os.getcwd(), "employee_management.db")
        logger.info(f"Initializing database at: {db_path}")
        
        # Create the database and tables
        create_employee_management_db()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False

def initialize_agent():
    """Initialize the LangGraph agent"""
    global agent
    try:
        agent = create_agent()
        logger.info("Agent initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize agent: {e}")
        return False

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Render"""
    try:
        # Check if database exists and is accessible
        if db_path and os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM employees")
            employee_count = cursor.fetchone()[0]
            conn.close()
            
            return jsonify({
                'status': 'healthy',
                'database': 'connected',
                'employee_count': employee_count,
                'timestamp': datetime.now().isoformat()
            }), 200
        else:
            return jsonify({
                'status': 'unhealthy',
                'database': 'not_found',
                'timestamp': datetime.now().isoformat()
            }), 500
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/query', methods=['POST'])
def handle_query():
    """Handle natural language queries through the agent"""
    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({'error': 'Query is required'}), 400
        
        query = data['query']
        logger.info(f"Received query: {query}")
        
        if not agent:
            return jsonify({'error': 'Agent not initialized'}), 500
        
        # Process the query through your agent
        # This is a simplified version - you'll need to adapt based on your agent's interface
        result = agent.invoke({"query": query})
        
        return jsonify({
            'query': query,
            'result': result,
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/database/stats', methods=['GET'])
def get_database_stats():
    """Get database statistics"""
    try:
        if not db_path or not os.path.exists(db_path):
            return jsonify({'error': 'Database not found'}), 404
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get basic statistics
        cursor.execute("SELECT COUNT(*) FROM employees")
        employee_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM departments")
        department_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM projects")
        project_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(salary) FROM employees")
        avg_salary = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'employees': employee_count,
            'departments': department_count,
            'projects': project_count,
            'average_salary': round(avg_salary, 2) if avg_salary else 0,
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/database/query', methods=['POST'])
def execute_sql_query():
    """Execute direct SQL queries (for testing purposes)"""
    try:
        data = request.get_json()
        if not data or 'sql' not in data:
            return jsonify({'error': 'SQL query is required'}), 400
        
        sql_query = data['sql']
        logger.info(f"Executing SQL: {sql_query}")
        
        if not db_path or not os.path.exists(db_path):
            return jsonify({'error': 'Database not found'}), 404
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Execute the query
        cursor.execute(sql_query)
        
        # Fetch results
        if sql_query.strip().upper().startswith('SELECT'):
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
        else:
            conn.commit()
            results = {'affected_rows': cursor.rowcount}
        
        conn.close()
        
        return jsonify({
            'sql': sql_query,
            'results': results,
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error executing SQL: {e}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/', methods=['GET'])
def index():
    """Main endpoint with API documentation"""
    return jsonify({
        'message': 'LocoForge API is running!',
        'endpoints': {
            'health': '/health',
            'query': '/api/query (POST)',
            'database_stats': '/api/database/stats (GET)',
            'sql_query': '/api/database/query (POST)'
        },
        'timestamp': datetime.now().isoformat()
    }), 200

if __name__ == '__main__':
    # Initialize database and agent on startup
    logger.info("Starting LocoForge application...")
    
    if initialize_database():
        logger.info("Database initialization completed")
    else:
        logger.error("Database initialization failed")
    
    if initialize_agent():
        logger.info("Agent initialization completed")
    else:
        logger.error("Agent initialization failed")
    
    # Get port from environment variable (Render sets this)
    port = int(os.environ.get('PORT', 5000))
    
    # Start the Flask app
    app.run(host='0.0.0.0', port=port, debug=False) 