from langgraph.graph import StateGraph, END, START
from my_agent.utils.state import OrchestratorState

from my_agent.utils.orchestrator_nodes import (
    classify_query_node,
    decompose_query_node,
    route_to_agents_node,
    sql_agent_node,
    nosql_agent_node,
    data_engineer_node,
    aggregate_results_node,
    update_context_node,
    format_response_node,
    route_decision,
    sql_agent_decision
)

def create_orchestrator_workflow():
    """Create the hybrid orchestrator workflow graph"""
    
    # Create the workflow
    workflow = StateGraph(OrchestratorState)
    
    # Add nodes
    workflow.add_node("classify_query", classify_query_node)
    workflow.add_node("decompose_query", decompose_query_node)
    workflow.add_node("route_to_agents", route_to_agents_node)
    workflow.add_node("sql_agent", sql_agent_node)
    workflow.add_node("nosql_agent", nosql_agent_node)
    workflow.add_node("data_engineer", data_engineer_node)
    workflow.add_node("aggregate_results", aggregate_results_node)
    workflow.add_node("update_context", update_context_node)
    workflow.add_node("format_response", format_response_node)
    
    # Add edges from START
    workflow.add_edge(START, "classify_query")
    workflow.add_edge("classify_query", "decompose_query")
    workflow.add_edge("decompose_query", "route_to_agents")
    
    # Add conditional edges based on routing decision
    workflow.add_conditional_edges(
        "route_to_agents",
        route_decision,
        {
            "sql_only": "sql_agent",
            "nosql_only": "nosql_agent", 
            "both_agents": "sql_agent",
            "data_engineer": "data_engineer",  # Route unclear queries to Data Engineer
            "error_handling": "format_response"
        }
    )
    
    # Add edges from agent nodes to aggregate_results
    workflow.add_edge("nosql_agent", "aggregate_results")
    workflow.add_edge("data_engineer", "aggregate_results")
    
    # Add conditional edge for both_agents to route to nosql_agent after sql_agent
    workflow.add_conditional_edges(
        "sql_agent",
        sql_agent_decision,
        {
            "nosql_agent": "nosql_agent",
            "aggregate_results": "aggregate_results"
        }
    )
    
    # Add final edges
    workflow.add_edge("aggregate_results", "update_context")
    workflow.add_edge("update_context", "format_response")
    workflow.add_edge("format_response", END)
    
    # Compile the graph
    return workflow.compile()

# Create the graph instance
graph = create_orchestrator_workflow()