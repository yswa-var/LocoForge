# NoSQL Query Executor (MongoDB)

A powerful NoSQL query executor that uses OpenAI to generate and execute MongoDB queries against a grocery warehouse database. This agent provides natural language to MongoDB query conversion with structured JSON output.

## Features

- **Natural Language to MongoDB**: Convert plain English queries to MongoDB find operations and aggregation pipelines
- **Comprehensive Database Context**: Built-in understanding of the grocery warehouse database schema
- **Structured JSON Output**: Returns results in a consistent, structured format
- **Interactive Mode**: Command-line interface for testing queries
- **Error Handling**: Robust error handling with detailed error messages
- **Connection Management**: Automatic MongoDB connection management

## Database Schema

The agent works with a **grocery warehouse database** containing three main collections:

### 1. Products Collection
- Product information (ID, name, category, brand, description)
- Specifications (weight, origin, allergens, nutritional info)
- Pricing (cost price, selling price, bulk discounts)
- Supplier information (supplier details, lead time)

### 2. Inventory Collection
- Stock levels and warehouse locations
- Batch tracking with expiry dates
- Movement history (in/out transactions)
- Quality status tracking

### 3. Orders Collection
- Customer information and order details
- Order items with pricing and discounts
- Payment information and status
- Delivery and shipping details

## Installation

1. **Install Dependencies**:
```bash
pip install pymongo langchain-openai python-dotenv
```

2. **Set Environment Variables**:
Create a `.env` file with:
```env
OPENAI_API_KEY=your_openai_api_key_here
MONGO_DB=mongodb://localhost:27017/
```

3. **Initialize Database**:
```bash
cd nosql_db_ops
python nosql_db_init.py
```

## Usage

### Interactive Mode
```bash
python my_agent/utils/nosql_agent.py
```

### Programmatic Usage
```python
from my_agent.utils.nosql_agent import create_nosql_agent

# Create agent
agent = create_nosql_agent()

# Execute a query
result = agent.generate_and_execute_query("Show all products with low stock")

# Access results
print(result["execution_result"]["data"])
```

### Testing
```bash
python test_nosql_agent.py
```

## Query Examples

### Simple Queries
- `"Show all products"`
- `"Find products in the Fruits category"`
- `"List high-value orders above $500"`

### Complex Queries
- `"Show products that need reordering (low stock)"`
- `"Find products expiring within 7 days"`
- `"Calculate total sales by product category"`
- `"Show warehouse zone utilization"`

## Output Format

The agent returns structured JSON with the following format:

```json
{
  "prompt": "Show all products",
  "generated_mongodb_query": "{ \"collection\": \"products\", \"query\": {}, \"projection\": { \"_id\": 0 } }",
  "execution_result": {
    "success": true,
    "query": "{ \"collection\": \"products\", \"query\": {}, \"projection\": { \"_id\": 0 } }",
    "row_count": 5,
    "data": [
      {
        "product_id": "PROD001",
        "name": "Organic Bananas",
        "category": "Fruits",
        "brand": "FreshHarvest"
      }
    ]
  },
  "timestamp": "2024-01-15T10:30:00.000Z"
}
```

## Supported Query Types

### Find Operations
- Simple field matching
- Nested object queries
- Array element matching
- Range queries
- Logical operators

### Aggregation Pipelines
- `$lookup` for joining collections
- `$match` for filtering
- `$group` for aggregations
- `$project` for field selection
- `$unwind` for array operations
- `$sort` for ordering
- `$limit` for result limiting

## Sample Queries

1. **Products with Low Stock**:
   ```python
   "Find products that need reordering (low stock)"
   ```

2. **High-Value Orders**:
   ```python
   "Show orders with total amount above $500"
   ```

3. **Expiring Products**:
   ```python
   "List products expiring within 7 days"
   ```

4. **Category Analysis**:
   ```python
   "Calculate total sales by product category"
   ```

5. **Customer Orders**:
   ```python
   "Find customers who made multiple orders"
   ```

## Error Handling

The agent handles various error scenarios:
- Invalid MongoDB queries
- Connection issues
- Missing collections
- Malformed JSON
- OpenAI API errors

## Configuration

### Environment Variables
- `OPENAI_API_KEY`: Your OpenAI API key
- `MONGO_DB`: MongoDB connection string

### Model Configuration
- Uses `gpt-4o-mini` for query generation
- Configurable model parameters in the agent class

## Database Setup

The grocery warehouse database includes:

### Sample Data
- 5 products with detailed specifications
- Inventory records with stock levels and batch tracking
- 3 orders with customer and payment information

### Indexes
- Product ID indexes for performance
- Category and status indexes
- Date-based indexes for time queries

## Comparison with SQL Agent

| Feature | SQL Agent | NoSQL Agent |
|---------|-----------|-------------|
| Database Type | SQLite | MongoDB |
| Query Language | SQL | MongoDB Query Language |
| Schema | Relational | Document-based |
| Joins | SQL JOINs | $lookup aggregation |
| Data Structure | Tables | Collections |
| Relationships | Foreign Keys | Embedded Documents |

## Troubleshooting

### Common Issues

1. **MongoDB Connection Error**:
   - Ensure MongoDB is running on localhost:27017
   - Check connection string in .env file

2. **OpenAI API Error**:
   - Verify API key is valid and has credits
   - Check internet connection

3. **Database Not Found**:
   - Run `nosql_db_init.py` to create the database
   - Ensure database name is "grocery_warehouse"

4. **Query Generation Errors**:
   - Check if the prompt is clear and specific
   - Verify database schema context is loaded

## Performance Tips

1. **Use Specific Queries**: More specific prompts generate better queries
2. **Limit Results**: Use "limit to 10" in prompts for large datasets
3. **Index Usage**: Queries automatically use database indexes
4. **Connection Pooling**: Agent manages MongoDB connections efficiently

## Future Enhancements

- Support for other NoSQL databases (Cassandra, Redis)
- Query optimization suggestions
- Query performance metrics
- Batch query processing
- Real-time data streaming
- Advanced aggregation pipelines

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

This project is part of the LocoForge framework and follows the same licensing terms. 