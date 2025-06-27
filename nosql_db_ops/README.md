# MongoDB Grocery Warehouse Database

This directory contains a comprehensive MongoDB database setup for a grocery warehouse management system. The database is designed to demonstrate complex NoSQL operations and relationships between collections.

## Database Structure

The database `grocery_warehouse` contains three main collections with complex interrelated data:

### 1. Products Collection
- **Complex nested structure** with product specifications, pricing, and supplier information
- **Features:**
  - Product details (ID, name, category, brand, description)
  - Detailed specifications (weight, origin, allergens, nutritional info)
  - Pricing information with bulk discounts
  - Supplier details with contact information and lead times
  - Timestamps and active status

### 2. Inventory Collection
- **Advanced inventory tracking** with batch management and movement history
- **Features:**
  - Warehouse location mapping (zone, aisle, shelf, position)
  - Stock level management (current, minimum, maximum, reorder points)
  - Batch tracking with expiry dates and quality status
  - Movement history with timestamps and references
  - Real-time stock updates

### 3. Orders Collection
- **Complex order management** with customer relationships and payment tracking
- **Features:**
  - Customer information with contact details and credit limits
  - Order details with status, priority, and delivery information
  - Itemized order lines with pricing and discounts
  - Payment processing with multiple methods
  - Comprehensive pricing calculations

## Installation and Setup

### Prerequisites
- MongoDB server running on localhost:27017
- Python 3.7+
- Required Python packages (see requirements.txt)

### Installation Steps

1. **Install MongoDB dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Ensure MongoDB is running:**
   ```bash
   # Start MongoDB (if not already running)
   mongod
   ```

3. **Create the database:**
   ```bash
   python nosql_db_init.py
   ```

4. **Test the database with complex queries:**
   ```bash
   python test_queries.py
   ```

## Database Features

### Complex Data Relationships
- **Product-Inventory Relationship:** Products linked to inventory records via product_id
- **Product-Order Relationship:** Products referenced in order items
- **Customer-Order Relationship:** Customer information embedded in orders
- **Supplier-Product Relationship:** Supplier details embedded in products

### Advanced NoSQL Capabilities Demonstrated

1. **Multi-Collection Aggregations**
   - Join products with inventory for stock analysis
   - Cross-reference orders with products for performance metrics
   - Complex grouping and aggregation operations

2. **Nested Object Queries**
   - Query nested specifications and pricing information
   - Filter by embedded supplier and customer details
   - Access deeply nested warehouse location data

3. **Array Operations**
   - Batch information tracking with expiry dates
   - Movement history with timestamps
   - Order items with pricing calculations
   - Quality status tracking across batches

4. **Date Range Operations**
   - Expiry date filtering for inventory management
   - Order date ranges for reporting
   - Movement history time-based queries

5. **Complex Filtering**
   - Stock level thresholds and reorder points
   - Payment status and method filtering
   - Quality status and batch filtering
   - Customer type and order priority filtering

## Sample Queries

The `test_queries.py` file demonstrates various complex queries:

### Basic Queries
- Find products needing reorder (stock below reorder point)
- Products expiring soon (within 7 days)
- High-value orders (above $500)

### Advanced Queries
- Warehouse zone utilization analysis
- Product category performance metrics
- Customer order pattern analysis
- Supplier performance evaluation
- Quality issue tracking
- Recent inventory movements

### Complex Aggregations
- Multi-collection joins for comprehensive reporting
- Grouping and aggregation for business intelligence
- Array filtering and element matching
- Nested object analysis and projection

## Database Statistics

After creation, the database contains:
- **5 Products** with complex specifications and supplier details
- **5 Inventory Records** with batch tracking and movement history
- **3 Orders** with customer information and payment processing
- **Total: 13 Documents** across all collections

## Performance Optimization

The database includes optimized indexes for:
- Product ID lookups
- Category and brand filtering
- Stock level queries
- Order status and date filtering
- Customer ID lookups
- Warehouse zone queries

## Error Handling

The database creation includes comprehensive error handling:
- Database existence checking
- Graceful recreation if database exists
- Connection error handling
- Index creation error handling
- Data insertion validation

## Usage in NoSQL Agent

This database is designed to showcase the capabilities of a NoSQL agent by providing:
- **Complex schema** that requires understanding of nested structures
- **Multiple relationships** that demonstrate join capabilities
- **Rich data types** including dates, arrays, and nested objects
- **Business logic** embedded in the data structure
- **Real-world scenarios** for inventory and order management

The agent can perform operations like:
- Stock level analysis and reorder recommendations
- Customer order pattern analysis
- Supplier performance evaluation
- Warehouse space utilization
- Quality control and expiry management
- Financial reporting and revenue analysis

## File Structure

```
nosql_db_ops/
├── nosql_db_init.py      # Database initialization and creation
├── test_queries.py       # Complex query demonstrations
├── requirements.txt      # Python dependencies
└── README.md            # This documentation
```

## Troubleshooting

### Common Issues

1. **MongoDB Connection Error:**
   - Ensure MongoDB is running on localhost:27017
   - Check if MongoDB service is started

2. **Import Errors:**
   - Install required packages: `pip install -r requirements.txt`
   - Ensure Python path includes the nosql_db_ops directory

3. **Database Creation Fails:**
   - Check MongoDB permissions
   - Ensure sufficient disk space
   - Verify MongoDB version compatibility

### Support

For issues with the database setup or queries, check:
- MongoDB logs for connection issues
- Python error messages for import or runtime issues
- Database statistics output for verification

## Next Steps

After setting up the database, you can:
1. Integrate with your NoSQL agent
2. Add more complex queries and operations
3. Extend the data model with additional collections
4. Implement real-time data updates
5. Add data validation and business rules 