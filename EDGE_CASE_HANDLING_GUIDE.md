# Enhanced Edge Case Handling with Data Engineer Agent

## Overview

The enhanced hybrid orchestrator now includes a **Data Engineer Agent** that provides seamless handling of unclear, irrelevant, and technical queries. This ensures users always receive professional, helpful responses regardless of query complexity or domain relevance.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   User Query    │───▶│ Query Analysis   │───▶│ Route Decision  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Clear Query     │◀───│ Domain Detection │    │ Unclear Query   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ SQL/NoSQL Agent │    │ Hybrid Agent     │    │ Data Engineer   │
│ Processing      │    │ Processing       │    │ Agent           │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Results         │    │ Aggregated       │    │ Professional    │
│ Formatting      │    │ Results          │    │ Response        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Query Classification

The system now classifies queries into multiple categories:

### 1. Clear Queries
- **Domain**: `employee`, `warehouse`, `hybrid`
- **Intent**: `select`, `analyze`, `compare`, `aggregate`
- **Handling**: Processed by SQL/NoSQL agents

### 2. Unclear Queries
- **Domain**: `unclear`
- **Intent**: `clarify`, `explain`
- **Types**:
  - **Ambiguous**: "Show me everything", "What's the data?"
  - **Non-Domain**: "What's the weather like?", "Tell me a joke"
  - **Technical**: "SELECT * FROM employees", "Show schema"
  - **Overly Complex**: Performance-impacting queries

## Data Engineer Agent Capabilities

### 1. Query Analysis
```python
analysis = data_engineer.analyze_query(query)
# Returns: {
#   "is_clear": boolean,
#   "query_type": "clear|ambiguous|non_domain|technical|overly_complex",
#   "domain_relevance": "employee|warehouse|hybrid|none",
#   "complexity_level": "simple|medium|complex|overly_complex",
#   "confidence": float,
#   "issues": [list of problems],
#   "suggested_domain": string
# }
```

### 2. Clarification Suggestions
For ambiguous queries, the agent provides specific suggestions:
```python
suggestions = data_engineer.provide_clarification_suggestions(query, analysis)
# Returns: [
#   "Show all employees in the company",
#   "List all products in the warehouse",
#   "Display all departments and their budgets",
#   "Show current inventory levels for all products"
# ]
```

### 3. Specialized Handlers

#### Technical Query Handler
- Handles SQL/NoSQL syntax questions
- Provides schema information
- Explains database structure
- Offers best practices

#### Non-Domain Query Handler
- Politely explains system capabilities
- Provides relevant examples
- Suggests alternative approaches
- Maintains professional tone

#### Complexity Handler
- Identifies performance issues
- Suggests query simplification
- Provides step-by-step approaches
- Explains optimization strategies

## Enhanced Workflow

### 1. Query Classification Node
```python
def classify_query_node(state: OrchestratorState) -> OrchestratorState:
    # Analyze query with Data Engineer Agent
    analysis = data_engineer.analyze_query(query)
    
    # Handle different query types
    if query_type in ["ambiguous", "non_domain", "technical", "overly_complex"]:
        state["query_domain"] = QueryDomain.UNCLEAR
        state["query_intent"] = QueryIntent.CLARIFY
        
        # Generate appropriate response
        if query_type == "ambiguous":
            suggestions = data_engineer.provide_clarification_suggestions(query, analysis)
            state["clarification_suggestions"] = suggestions
        elif query_type == "technical":
            response = data_engineer.handle_technical_query(query)
            state["data_engineer_response"] = response
        # ... handle other types
```

### 2. Data Engineer Node
```python
def data_engineer_node(state: OrchestratorState) -> OrchestratorState:
    # Format professional response
    response_content = data_engineer_response["response"]
    
    # Add clarification suggestions
    if clarification_suggestions:
        response_content += "\n\nHere are some suggestions to help clarify your query:\n"
        for i, suggestion in enumerate(clarification_suggestions, 1):
            response_content += f"{i}. {suggestion}\n"
    
    # Create combined results
    state["combined_results"] = {
        "success": True,
        "original_query": query,
        "query_type": "unclear",
        "response": response_content,
        "clarification_suggestions": clarification_suggestions,
        "timestamp": timestamp
    }
```

### 3. Routing Decision
```python
def route_decision(state: OrchestratorState) -> str:
    domain = state["query_domain"]
    
    if domain == QueryDomain.EMPLOYEE:
        return "sql_only"
    elif domain == QueryDomain.WAREHOUSE:
        return "nosql_only"
    elif domain == QueryDomain.HYBRID:
        return "both_agents"
    elif domain == QueryDomain.UNCLEAR:
        return "data_engineer"  # New routing path
    else:
        return "error_handling"
```

## Example Responses

### Ambiguous Query: "Show me everything"
```
I understand you'd like to see data, but "everything" is quite broad. 
Let me help you find what you're looking for.

Here are some suggestions to help clarify your query:
1. Show all employees in the company
2. List all products in the warehouse
3. Display all departments and their budgets
4. Show current inventory levels for all products
5. List all active projects and their status

Which of these would be most helpful, or would you like to explore a specific area?
```

### Non-Domain Query: "What's the weather like?"
```
I understand you're asking about the weather. This system is specifically designed for 
employee management and warehouse operations.

Here are some examples of what I can help you with:
- Employee data: salaries, departments, attendance, projects
- Warehouse data: products, inventory, orders, suppliers
- Cross-domain analysis: employee ordering patterns, department spending

Would you like to explore any of these areas instead?
```

### Technical Query: "SELECT * FROM employees"
```
I see you're using SQL syntax. This system supports SQL queries for the employee 
management database. Here's what you can query:

**Available Tables:**
- employees: employee information, salaries, positions
- departments: department data, budgets, locations
- projects: project assignments, status, budgets
- attendance: attendance records, overtime

**Example Queries:**
- Show all employees: "List all employees"
- Find IT employees: "Show employees in IT department"
- High salary employees: "Find employees with salary above $50,000"

Would you like me to help you with a specific employee-related query?
```

### Overly Complex Query: "Find everything about everything"
```
This query is quite complex and may impact system performance. Here's why:

1. Multiple joins across different domains
2. Large result sets expected
3. Complex filtering conditions

I recommend breaking this down into smaller, focused queries:
1. First, get employee data: "Show all employees with their departments"
2. Then, get warehouse data: "List all products with current inventory"
3. Finally, combine results: "Show employee ordering patterns"

This approach will be more efficient and easier to debug.
```

## Benefits

### 1. Seamless User Experience
- No more error messages for unclear queries
- Professional, helpful responses
- Clear guidance and suggestions

### 2. Improved Query Success Rate
- Handles edge cases gracefully
- Provides clarification for ambiguous requests
- Offers alternatives for non-domain queries

### 3. Performance Protection
- Identifies overly complex queries
- Suggests optimization strategies
- Prevents system overload

### 4. Professional Communication
- Context-aware responses
- Technical guidance when appropriate
- Polite handling of off-topic queries

## Testing

Run the comprehensive test suite:
```bash
python test_edge_cases.py
```

This will test:
- All edge case scenarios
- Data Engineer Agent functionality
- Response quality and relevance
- System performance under various query types

## Configuration

The Data Engineer Agent can be customized by modifying:
- `database_context` in `DataEngineerAgent.__init__()`
- System prompts in `_get_*_prompt()` methods
- Response formatting in `data_engineer_node()`

## Future Enhancements

1. **Learning from Interactions**: Track which suggestions users follow
2. **Dynamic Context**: Update database context based on schema changes
3. **Multi-language Support**: Handle queries in different languages
4. **Query Templates**: Provide reusable query patterns
5. **Performance Analytics**: Track query complexity and optimization success

## Conclusion

The enhanced edge case handling system provides a professional, seamless experience for all users, regardless of their query complexity or domain knowledge. The Data Engineer Agent ensures that every interaction is helpful and productive, maintaining the system's reliability while expanding its accessibility. 