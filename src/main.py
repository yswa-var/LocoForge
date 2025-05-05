from typing import Annotated, TypedDict, Dict, Any, List
from langgraph.graph import Graph, StateGraph
from langgraph.prebuilt import ToolNode
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_openai import ChatOpenAI
import os
from dotenv import load_dotenv
import json
import pandas as pd
from datetime import datetime
from logger import default_logger as logger

from llm_router import task_creation_agent, execute_task
from sql_agent import SQLAgent
from nosql_agent import GeneralizedNoSQLAgent, MongoJSONEncoder
from google_drive_ops import DriveAgent

# Load environment variables
load_dotenv()

# Initialize the LLM
llm = ChatOpenAI(
    model="gpt-3.5-turbo", 
    temperature=0.7,
    api_key=os.getenv("OPENAI_API_KEY")
)

# Custom JSON encoder to handle datetime objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Define the state type for our graph
class GraphState(TypedDict):
    messages: List[HumanMessage | AIMessage | SystemMessage]
    task_spec: Dict[str, Any]
    results: Dict[str, Any]
    error: str | None
    drive_agent: DriveAgent | None  # Add DriveAgent to state

# Node for task creation
def create_task_node(state: GraphState) -> GraphState:
    """Create a task specification from the user's message."""
    try:
        # Get the last user message
        last_message = state["messages"][-1]
        if not isinstance(last_message, HumanMessage):
            raise ValueError("Last message must be from user")
        
        # Create task specification
        task_spec = task_creation_agent(last_message.content)
        
        return {
            "messages": state["messages"],
            "task_spec": task_spec,
            "results": {},
            "error": None,
            "drive_agent": state.get("drive_agent")
        }
    except Exception as e:
        return {
            "messages": state["messages"],
            "task_spec": {},
            "results": {},
            "error": str(e),
            "drive_agent": state.get("drive_agent")
        }

# Node for task execution
async def execute_task_node(state: GraphState) -> GraphState:
    """Execute the task using the appropriate agent(s)."""
    try:
        if state["error"]:
            return state
        
        # Initialize agents
        sql_agent = SQLAgent("sales.db")
        nosql_agent = GeneralizedNoSQLAgent()
        
        # Initialize DriveAgent if not already present
        if not state.get("drive_agent"):
            state["drive_agent"] = DriveAgent(auth_method='oauth')
        
        # Execute task
        results = await execute_task(state["task_spec"], sql_agent, nosql_agent, state["drive_agent"])
        
        # Create structured response
        response_data = {
            "status": "success",
            "agent_type": state["task_spec"]["agent_type"],
            "task_type": state["task_spec"]["task_type"],
            "results": {}
        }
        
        # Convert results to pandas DataFrames and structure the response
        if results.get("results"):
            for agent_type, result in results["results"].items():
                try:
                    if isinstance(result, (list, dict)):
                        # Convert to DataFrame
                        if isinstance(result, dict):
                            # If it's a single dictionary, convert to list of one item
                            df = pd.DataFrame([result])
                        else:
                            # If it's already a list, convert directly
                            df = pd.DataFrame(result)
                        # Convert DataFrame to dict for JSON serialization
                        response_data["results"][agent_type] = {
                            "data": df.to_dict(orient='records'),
                            "columns": df.columns.tolist(),
                            "row_count": len(df)
                        }
                    else:
                        # Keep original result if not convertible to DataFrame
                        response_data["results"][agent_type] = result
                except Exception as e:
                    logger.error(f"Error converting {agent_type} results to DataFrame: {str(e)}")
                    # Keep the original result if conversion fails
                    response_data["results"][agent_type] = result
        
        # Add response to messages
        response = AIMessage(content=json.dumps(response_data, indent=2, cls=CustomJSONEncoder))
        
        return {
            "messages": state["messages"] + [response],
            "task_spec": state["task_spec"],
            "results": response_data,
            "error": None,
            "drive_agent": state["drive_agent"]
        }
    except Exception as e:
        logger.error(f"Error in execute_task_node: {str(e)}", exc_info=True)
        error_response = {
            "status": "error",
            "error": str(e),
            "message": "An error occurred while processing your request"
        }
        return {
            "messages": state["messages"] + [AIMessage(content=json.dumps(error_response, indent=2, cls=CustomJSONEncoder))],
            "task_spec": state["task_spec"],
            "results": error_response,
            "error": str(e),
            "drive_agent": state.get("drive_agent")
        }
    finally:
        if 'sql_agent' in locals():
            sql_agent.close()
        if 'nosql_agent' in locals():
            nosql_agent.close()
        #drive_agent cleanup 
        if state.get("drive_agent") and "drive" in state["task_spec"].get("agent_type", ""):
            asyncio.create_task(state["drive_agent"].close())

# Node for error handling
def error_handler_node(state: GraphState) -> GraphState:
    """Handle any errors that occurred during task creation or execution."""
    if state["error"]:
        error_parts = state["error"].split(":")
        if len(error_parts) > 1:
            error_type = error_parts[0].strip()
            error_detail = ":".join(error_parts[1:]).strip()
        else:
            error_type = "Error"
            error_detail = state["error"]
            
        error_response = {
            "status": "error",
            "error_type": error_type,
            "error_detail": error_detail,
            "suggestions": ["Try rephrasing your request", "Be more specific", "Check your input data"]
        }
            
        error_message = AIMessage(content=json.dumps(error_response, indent=2))
        return {
            "messages": state["messages"] + [error_message],
            "task_spec": state["task_spec"],
            "results": state["results"],
            "error": None,
            "drive_agent": state.get("drive_agent")
        }
    return state

#For cleaning up resources before finishing
async def cleanup_resources_node(state: GraphState) -> GraphState:
    """Clean up resources before finishing."""
    if state.get("drive_agent"):
        try:
            await state["drive_agent"].close()
            logger.info("Drive agent resources cleaned up")
        except Exception as e:
            logger.error(f"Error cleaning up drive agent: {str(e)}")
    return state

# Create the graph
def create_graph() -> Graph:
    # Initialize the graph
    workflow = StateGraph(GraphState)
    
    # Add nodes
    workflow.add_node("create_task", create_task_node)
    workflow.add_node("execute_task", execute_task_node)
    workflow.add_node("error_handler", error_handler_node)
    workflow.add_node("cleanup_resources", cleanup_resources_node)
    
    # Define edges
    workflow.add_edge("create_task", "execute_task")
    workflow.add_edge("execute_task", "error_handler")
    workflow.add_edge("error_handler", "cleanup_resources")
    
    # Set the entry point
    workflow.set_entry_point("create_task")
    
    # Set the exit point
    workflow.set_finish_point("cleanup_resources")
    
    # Compile the graph
    compiled_graph = workflow.compile()
    
    return compiled_graph

async def main():
    """Main function to demonstrate the workflow."""
    # Create the graph
    graph = create_graph()
    
    # Example queries
    example_queries = [
        # User-related queries (NoSQL)
        "Find all users who logged in within the last 24 hours",
        "Update the role of user 'john.doe@example.com' to 'admin'",
        
        # Business-related queries (SQL)
        "Show me total sales for the last quarter",
        "List all products with inventory below 100 units",
        
        # Combined queries (Both)
        "Find all users who made purchases above $1000 and their purchase history",
        
        # Google Drive queries
        "List all files in my Google Drive",
        "Search for documents containing 'project proposal'",
        "Create a new folder named 'Project Documents'"
    ]
    
    # Process each query
    for query in example_queries:
        print(f"\n{'='*50}")
        print(f"Processing query: {query}")
        print(f"{'='*50}")
        
        # Initialize the state
        initial_state = {
            "messages": [HumanMessage(content=query)],
            "task_spec": {},
            "results": {},
            "error": None,
            "drive_agent": None  # Initialize drive_agent as None
        }
        
        # Run the graph
        result = await graph.ainvoke(initial_state)
        
        # Print the results
        print("\nFinal messages:")
        for message in result["messages"]:
            print(f"\n{message.type}: {message.content}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
