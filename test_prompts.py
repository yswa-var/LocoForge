"""
Comprehensive Test Prompts for Hybrid Orchestrator
Tests various scenarios: SQL-only, NoSQL-only, Hybrid, and Edge Cases
"""

# ============================================================================
# SQL-ONLY PROMPTS (Employee Management Domain)
# ============================================================================

SQL_ONLY_PROMPTS = [
    # Basic Employee Queries
    "Show me all employees",
    "List all employees in the IT department",
    "Find employees with salary above $50,000",
    "Show employees hired in the last 6 months",
    "Display all managers and their subordinates",
    
    # Department Queries
    "What departments exist in the company?",
    "Show department budgets and locations",
    "Find departments with more than 10 employees",
    "List departments by average salary",
    
    # Project Queries
    "Show all active projects",
    "Find projects with budgets over $100,000",
    "List employees working on project 'Alpha'",
    "Show project completion status",
    
    # Attendance Queries
    "Show attendance records for this month",
    "Find employees with perfect attendance",
    "Display late arrivals this week",
    "Show overtime hours by employee",
    
    # Complex SQL Queries
    "Find employees who are managers and have subordinates in different departments",
    "Show departments with highest average salary",
    "List employees who work on multiple projects",
    "Find employees with salary above department average",
    "Show project managers and their project budgets"
]

# ============================================================================
# NOSQL-ONLY PROMPTS (Warehouse Management Domain)
# ============================================================================

NOSQL_ONLY_PROMPTS = [
    # Basic Product Queries
    "Show all products in the warehouse",
    "List products in the Fruits category",
    "Find products with low stock levels",
    "Show organic certified products",
    "Display products from specific suppliers",
    
    # Inventory Queries
    "Show current inventory levels",
    "Find items below reorder point",
    "List products expiring this month",
    "Show inventory by warehouse zone",
    "Display batch information for products",
    
    # Order Queries
    "Show all pending orders",
    "Find orders from last week",
    "List high-priority orders",
    "Show customer order history",
    "Display orders by delivery method",
    
    # Supplier Queries
    "List all suppliers",
    "Find suppliers with fastest delivery",
    "Show supplier contact information",
    "Display products by supplier",
    
    # Complex NoSQL Queries
    "Find products with multiple batches",
    "Show inventory movement history",
    "List products with bulk discounts",
    "Find orders with special instructions",
    "Show products by nutritional information"
]

# ============================================================================
# HYBRID PROMPTS (Cross-Domain Queries)
# ============================================================================

HYBRID_PROMPTS = [
    # Employee-Product Relationships
    "Find employees who ordered products that are low in stock",
    "Show which employees placed orders for organic products",
    "List employees who ordered products from specific suppliers",
    "Find employees who ordered high-value products",
    
    # Department-Inventory Analysis
    "Show departments that ordered products with low inventory",
    "Find departments with highest order volumes",
    "List departments that ordered products expiring soon",
    "Show which departments prefer organic products",
    
    # Project-Order Analysis
    "Find projects that required warehouse products",
    "Show project managers who placed large orders",
    "List projects with specific product requirements",
    "Find projects that ordered products from multiple suppliers",
    
    # Attendance-Inventory Correlation
    "Find employees with attendance issues who also placed orders",
    "Show employees who worked overtime and placed urgent orders",
    "List employees with perfect attendance who ordered regularly",
    
    # Complex Cross-Domain Queries
    "Find managers whose departments ordered products that are now low in stock",
    "Show employees who work on multiple projects and placed orders for those projects",
    "Find departments with high budgets that also placed large warehouse orders",
    "List employees with salary above average who ordered premium products",
    "Show project managers who ordered products that are expiring soon"
]

# ============================================================================
# EDGE CASES AND ERROR SCENARIOS
# ============================================================================

EDGE_CASE_PROMPTS = [
    # Ambiguous Queries
    "Show me everything",
    "What's the data?",
    "Give me information",
    "List all records",
    
    # Non-Domain Queries
    "What's the weather like?",
    "Tell me a joke",
    "How do I cook pasta?",
    "What's the capital of France?",
    
    # Complex Multi-Domain
    "Find employees in IT department who ordered organic fruits that are low in stock and have managers with high salaries",
    "Show projects managed by employees who ordered products from suppliers with fast delivery times and have perfect attendance records",
    
    # Specific Technical Queries
    "SELECT * FROM employees",
    "db.products.find()",
    "Show me the database schema",
    "What tables exist?",
    
    # Performance Testing
    "Show all employees with all their details and all projects and all attendance records and all orders and all products",
    "Find everything about everything in the system"
]

# ============================================================================
# ANALYSIS AND REPORTING PROMPTS
# ============================================================================

ANALYSIS_PROMPTS = [
    # Business Intelligence
    "Generate a report on employee productivity vs order patterns",
    "Analyze the correlation between department budgets and warehouse spending",
    "Show trends in employee ordering behavior",
    "Create a summary of cross-departmental warehouse usage",
    
    # Performance Metrics
    "Calculate average order value by department",
    "Show employee efficiency based on attendance and order accuracy",
    "Analyze supplier performance by employee satisfaction",
    "Find departments with best cost management",
    
    # Predictive Analysis
    "Predict which products will be ordered based on employee patterns",
    "Identify employees likely to place large orders",
    "Forecast inventory needs based on employee ordering history",
    "Predict which departments will need more budget for orders"
]

# ============================================================================
# COMPARISON AND BENCHMARKING PROMPTS
# ============================================================================

COMPARISON_PROMPTS = [
    # Department Comparisons
    "Compare ordering patterns between IT and HR departments",
    "Show which departments order the most organic products",
    "Compare employee attendance vs order frequency by department",
    "Find departments with highest order values",
    
    # Employee Comparisons
    "Compare ordering behavior between managers and regular employees",
    "Show employees with highest order volumes vs their salaries",
    "Compare attendance records with order patterns",
    "Find employees who order the most premium products",
    
    # Product Comparisons
    "Compare organic vs non-organic product ordering by department",
    "Show which product categories are most popular by employee type",
    "Compare supplier performance by employee satisfaction",
    "Find products with highest order frequency by department"
]

# ============================================================================
# SEARCH AND FILTER PROMPTS
# ============================================================================

SEARCH_PROMPTS = [
    # Text Search
    "Find employees named John",
    "Search for products containing 'organic'",
    "Find orders with 'urgent' in the description",
    "Search for suppliers with 'fresh' in their name",
    
    # Date Range Queries
    "Show orders from January 2024",
    "Find employees hired between 2020 and 2023",
    "Show inventory movements in the last 30 days",
    "Find projects started in Q1 2024",
    
    # Range Queries
    "Find products with prices between $10 and $50",
    "Show employees with salaries between $40k and $80k",
    "Find orders with quantities between 100 and 500",
    "Show projects with budgets between $50k and $200k"
]

# ============================================================================
# AGGREGATION AND STATISTICS PROMPTS
# ============================================================================

AGGREGATION_PROMPTS = [
    # Count Queries
    "How many employees are in each department?",
    "Count products by category",
    "How many orders were placed last month?",
    "Count employees by position",
    
    # Sum Queries
    "What's the total budget across all departments?",
    "Calculate total order value by customer",
    "Sum up all employee salaries",
    "Calculate total inventory value",
    
    # Average Queries
    "What's the average salary by department?",
    "Calculate average order value",
    "Show average attendance rate by employee",
    "Find average product price by supplier",
    
    # Min/Max Queries
    "Find the highest paid employee",
    "Show the most expensive product",
    "Find the department with lowest budget",
    "Show the largest order ever placed"
]

# ============================================================================
# ALL PROMPTS COMBINED
# ============================================================================

ALL_TEST_PROMPTS = {
    "sql_only": SQL_ONLY_PROMPTS,
    "nosql_only": NOSQL_ONLY_PROMPTS,
    "hybrid": HYBRID_PROMPTS,
    "edge_cases": EDGE_CASE_PROMPTS,
    "analysis": ANALYSIS_PROMPTS,
    "comparison": COMPARISON_PROMPTS,
    "search": SEARCH_PROMPTS,
    "aggregation": AGGREGATION_PROMPTS
}

# ============================================================================
# QUICK TEST SUITE
# ============================================================================

QUICK_TEST_SUITE = [
    # Essential Tests (5 queries)
    "Show me all employees",  # SQL only
    "What products are low in stock?",  # NoSQL only
    "Find employees who ordered products that are running low",  # Hybrid
    "How many departments exist?",  # SQL aggregation
    "Show orders from last week",  # NoSQL date filter
]

# ============================================================================
# COMPLEXITY-BASED TEST SUITES
# ============================================================================

SIMPLE_TESTS = [
    "Show all employees",
    "List all products",
    "What departments exist?",
    "Show current inventory",
    "Find all orders"
]

MEDIUM_TESTS = [
    "Find employees in IT department",
    "Show products with low stock",
    "List orders from last month",
    "Find managers and their subordinates",
    "Show products by category"
]

COMPLEX_TESTS = [
    "Find employees who ordered products that are low in stock",
    "Show departments that ordered organic products",
    "Find project managers who placed large orders",
    "Compare ordering patterns between departments",
    "Analyze employee productivity vs order patterns"
]

# ============================================================================
# FUNCTION TO RUN TESTS
# ============================================================================

def run_test_suite(test_prompts, suite_name="Test Suite"):
    """
    Run a test suite and return results
    
    Args:
        test_prompts: List of prompts to test
        suite_name: Name of the test suite
        
    Returns:
        Dictionary with test results
    """
    print(f"🧪 Running {suite_name}")
    print(f"📊 Total prompts: {len(test_prompts)}")
    print("=" * 60)
    
    results = {
        "suite_name": suite_name,
        "total_prompts": len(test_prompts),
        "successful": 0,
        "failed": 0,
        "errors": [],
        "results": []
    }
    
    for i, prompt in enumerate(test_prompts, 1):
        print(f"\n{i:2d}. Testing: {prompt}")
        try:
            # Import here to avoid circular imports
            from test_orchestrator import process_query
            result = process_query(prompt)
            
            if result.get("error"):
                print(f"   ❌ Error: {result['error']}")
                results["failed"] += 1
                results["errors"].append({
                    "prompt": prompt,
                    "error": result["error"]
                })
            else:
                print(f"   ✅ Success: {result['domain']} domain")
                results["successful"] += 1
            
            results["results"].append(result)
            
        except Exception as e:
            print(f"   💥 Exception: {str(e)}")
            results["failed"] += 1
            results["errors"].append({
                "prompt": prompt,
                "error": str(e)
            })
    
    print(f"\n📈 Results Summary:")
    print(f"   ✅ Successful: {results['successful']}")
    print(f"   ❌ Failed: {results['failed']}")
    print(f"   📊 Success Rate: {(results['successful']/len(test_prompts)*100):.1f}%")
    
    return results

def print_prompt_categories():
    """Print all available prompt categories"""
    print("📋 Available Test Prompt Categories:")
    print("=" * 50)
    
    for category, prompts in ALL_TEST_PROMPTS.items():
        print(f"🔹 {category.replace('_', ' ').title()}: {len(prompts)} prompts")
    
    print(f"\n🔹 Quick Test Suite: {len(QUICK_TEST_SUITE)} prompts")
    print(f"🔹 Simple Tests: {len(SIMPLE_TESTS)} prompts")
    print(f"🔹 Medium Tests: {len(MEDIUM_TESTS)} prompts")
    print(f"🔹 Complex Tests: {len(COMPLEX_TESTS)} prompts")

if __name__ == "__main__":
    print_prompt_categories()
    
    # Run quick test suite
    print("\n" + "="*60)
    run_test_suite(QUICK_TEST_SUITE, "Quick Test Suite") 