from typing import Annotated, TypedDict, Dict, Any, List
from langgraph.graph import Graph, StateGraph
from langgraph.prebuilt import ToolNode
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_openai import ChatOpenAI
import os
from dotenv import load_dotenv
import json
import pandas as pd

from llm_router import task_creation_agent, execute_task
from sql_agent import SQLAgent
from nosql_agent import GeneralizedNoSQLAgent, MongoJSONEncoder

# Load environment variables
load_dotenv()

# Initialize the LLM
llm = ChatOpenAI(
    model="gpt-3.5-turbo", 
    temperature=0.7,
    api_key=os.getenv("OPENAI_API_KEY")
)

# Define the state type for our graph
class GraphState(TypedDict):
    messages: List[HumanMessage | AIMessage | SystemMessage]
    task_spec: Dict[str, Any]
    results: Dict[str, Any]
    error: str | None

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
            "error": None
        }
    except Exception as e:
        return {
            "messages": state["messages"],
            "task_spec": {},
            "results": {},
            "error": str(e)
        }

# Node for task execution
def execute_task_node(state: GraphState) -> GraphState:
    """Execute the task using the appropriate agent(s)."""
    try:
        if state["error"]:
            return state
        
        # Initialize agents
        sql_agent = SQLAgent("sales.db")
        nosql_agent = GeneralizedNoSQLAgent()
        
        # Execute task
        results = execute_task(state["task_spec"], sql_agent, nosql_agent)
        
        # Create response message
        response_content = f"Task executed successfully.\n\n"
        response_content += f"Agent Type: {state['task_spec']['agent_type']}\n"
        response_content += f"Task Type: {state['task_spec']['task_type']}\n\n"
        
        # Convert results to pandas DataFrames
        if results["results"]:
            for agent_type, result in results["results"].items():
                if isinstance(result, (list, dict)):
                    # Convert to DataFrame
                    if isinstance(result, dict):
                        # If it's a single dictionary, convert to list of one item
                        df = pd.DataFrame([result])
                    else:
                        # If it's already a list, convert directly
                        df = pd.DataFrame(result)
                    results["results"][agent_type] = df
        
        # Add response to messages
        response = AIMessage(content=response_content)
        
        return {
            "messages": state["messages"] + [response],
            "task_spec": state["task_spec"],
            "results": results,
            "error": None
        }
    except Exception as e:
        return {
            "messages": state["messages"],
            "task_spec": state["task_spec"],
            "results": {},
            "error": str(e)
        }
    finally:
        if 'sql_agent' in locals():
            sql_agent.close()
        if 'nosql_agent' in locals():
            nosql_agent.close()

# Node for error handling
def error_handler_node(state: GraphState) -> GraphState:
    """Handle any errors that occurred during task creation or execution."""
    if state["error"]:
        error_message = AIMessage(content=f"An error occurred: {state['error']}")
        return {
            "messages": state["messages"] + [error_message],
            "task_spec": state["task_spec"],
            "results": state["results"],
            "error": None
        }
    return state

# Create the graph
def create_graph() -> Graph:
    # Initialize the graph
    workflow = StateGraph(GraphState)
    
    # Add nodes
    workflow.add_node("create_task", create_task_node)
    workflow.add_node("execute_task", execute_task_node)
    workflow.add_node("error_handler", error_handler_node)
    
    # Define edges
    workflow.add_edge("create_task", "execute_task")
    workflow.add_edge("execute_task", "error_handler")
    
    # Set the entry point
    workflow.set_entry_point("create_task")
    
    # Set the exit point
    workflow.set_finish_point("error_handler")
    
    # Compile the graph
    return workflow.compile()

def main():
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
        "Find all users who made purchases above $1000 and their purchase history"
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
            "error": None
        }
        
        # Run the graph
        result = graph.invoke(initial_state)
        
        # Print the results
        print("\nFinal messages:")
        for message in result["messages"]:
            print(f"\n{message.type}: {message.content}")

if __name__ == "__main__":
    main()
