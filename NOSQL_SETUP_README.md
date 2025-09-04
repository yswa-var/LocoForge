# NoSQL Database Setup with Faker Data Generation

This document describes how to set up and populate the NoSQL database (`sample_supplies`) with realistic fake data using the Faker library.

## Overview

The NoSQL database setup creates a comprehensive sample database for the LocoForge orchestrator system. It generates realistic sales data with customers, products, and transactions across multiple categories and locations.

## Features

- ✅ **Realistic Data Generation**: Uses Faker library for authentic-looking data
- ✅ **Multiple Product Categories**: 5 categories with 50+ products
- ✅ **Geographic Distribution**: 20 major US cities as store locations
- ✅ **Customer Demographics**: Age, gender, satisfaction ratings
- ✅ **Seasonal Patterns**: Realistic sales patterns throughout the year
- ✅ **Database Indexing**: Optimized for query performance
- ✅ **Batch Processing**: Efficient handling of large datasets

## Database Schema

### Sales Collection Structure

```javascript
{
  couponUsed: Boolean,
  customer: {
    age: Integer (18-80),
    email: String (fake email),
    gender: String (M/F),
    satisfaction: Integer (1-5)
  },
  items: [
    {
      name: String (product name),
      price: Decimal128 (realistic price),
      quantity: Integer (1-10),
      tags: [String] (category tags)
    }
  ],
  purchaseMethod: String (Online/In store/Phone/Mobile app),
  saleDate: DateTime (last 2 years),
  storeLocation: String (20 major cities)
}
```

## Product Categories

### 1. Office Supplies
- **Items**: Pencil, Pen, Notebook, Stapler, Paper clips, Tape, Scissors, Ruler, Calculator, Desk organizer
- **Tags**: office, stationery, desk, work
- **Price Range**: $2-50

### 2. Electronics
- **Items**: Laptop, Mouse, Keyboard, Monitor, Headphones, USB cable, Power adapter, Webcam, Microphone, Tablet
- **Tags**: electronics, tech, computer, digital
- **Price Range**: $50-2000

### 3. Home & Garden
- **Items**: Garden hose, Plant pot, Seeds, Fertilizer, Garden tools, Outdoor lights, Bird feeder, Watering can, Pruning shears, Mulch
- **Tags**: garden, outdoor, plants, home
- **Price Range**: $20-500

### 4. Kitchen & Dining
- **Items**: Coffee maker, Toaster, Blender, Microwave, Dishware, Utensils, Cookware, Food storage, Spice rack, Wine glasses
- **Tags**: kitchen, cooking, dining, food
- **Price Range**: $15-300

### 5. Sports & Outdoors
- **Items**: Yoga mat, Dumbbells, Running shoes, Water bottle, Backpack, Tent, Sleeping bag, Camping stove, Hiking boots, Bicycle
- **Tags**: sports, fitness, outdoor, exercise
- **Price Range**: $25-400

## Store Locations

The system uses 20 major US cities:
- New York, Los Angeles, Chicago, Houston, Phoenix
- Philadelphia, San Antonio, San Diego, Dallas, San Jose
- Austin, Jacksonville, Fort Worth, Columbus, Charlotte
- San Francisco, Indianapolis, Seattle, Denver, Washington

## Purchase Methods

- Online
- In store
- Phone
- Mobile app

## Installation

### 1. Install Dependencies

```bash
# Activate virtual environment
source venv/bin/activate

# Install Faker library
pip install Faker
```

### 2. Set Up Environment Variables

Ensure your `.env` file contains:

```bash
# MongoDB connection
MONGO_DB=mongodb://localhost:27017/

# OpenAI API key (for query generation)
OPENAPI_KEY=your_openai_api_key_here
```

### 3. Start MongoDB

```bash
# On macOS with Homebrew
brew services start mongodb-community

# Or start MongoDB manually
mongod
```

## Usage

### Basic Setup

Run the setup script:

```bash
python my_agent/utils/setup_nosql_db.py
```

Choose from the following options:

1. **Clear existing data and populate with new data**
   - Removes all existing data
   - Generates new sales records
   - Creates database indexes

2. **Add to existing data**
   - Keeps existing data
   - Adds additional sales records

3. **Generate seasonal data**
   - Creates data with realistic seasonal patterns
   - Higher sales during holidays and back-to-school periods

4. **Show database statistics**
   - Displays current database metrics
   - Shows total sales, average values, etc.

5. **Create indexes only**
   - Creates performance indexes without data generation

### Example Usage

```bash
# Generate 1000 sales records
python my_agent/utils/setup_nosql_db.py
# Choose option 1 and enter 1000

# Generate seasonal data for 2024
python my_agent/utils/setup_nosql_db.py
# Choose option 3 and enter 2024

# Check database statistics
python my_agent/utils/setup_nosql_db.py
# Choose option 4
```

## Testing

### Run Mock Tests (No MongoDB Required)

```bash
python test_nosql_setup_mock.py
```

This tests the data generation logic without requiring a MongoDB connection.

### Run Full Tests (MongoDB Required)

```bash
python test_nosql_setup.py
```

This tests the complete setup including database connection and data insertion.

### View Demonstration

```bash
python demo_nosql_setup.py
```

Shows a comprehensive overview of the setup features and capabilities.

## Sample Queries

Once the database is populated, you can run queries like:

- "Show all sales with coupons used"
- "Find sales with high customer satisfaction (4 or 5)"
- "Show online purchases"
- "List sales by store location"
- "Find sales with specific items like pencils"
- "Show sales by customer age groups"
- "List total sales by item"
- "Find sales with specific tags"
- "Show average satisfaction by gender"
- "Find sales from specific date ranges"

## Data Generation Features

### Realistic Patterns

- **Coupon Usage**: 30% of sales use coupons
- **Customer Demographics**: Realistic age distribution (18-80)
- **Satisfaction Ratings**: 1-5 scale with realistic distribution
- **Item Quantities**: 1-10 items per sale
- **Date Distribution**: Sales spread over the last 2 years

### Seasonal Variations

- **Holiday Season** (Nov-Dec): Higher sales volume
- **Summer** (Jun-Aug): Moderate sales
- **Back to School** (Sep-Oct): Increased office supplies
- **Regular Months**: Standard sales volume

### Performance Optimizations

- **Batch Processing**: Inserts data in batches of 100 records
- **Database Indexes**: Created on commonly queried fields
- **Efficient Queries**: Optimized MongoEngine queries

## Troubleshooting

### Common Issues

1. **MongoDB Connection Failed**
   ```
   Solution: Ensure MongoDB is running
   brew services start mongodb-community
   ```

2. **Faker Import Error**
   ```
   Solution: Install Faker library
   pip install Faker
   ```

3. **Environment Variables Missing**
   ```
   Solution: Check .env file contains MONGO_DB
   ```

4. **Permission Errors**
   ```
   Solution: Ensure proper MongoDB permissions
   ```

### Performance Tips

- Use batch processing for large datasets (>1000 records)
- Create indexes for frequently queried fields
- Monitor database size and performance
- Use appropriate MongoDB connection settings

## Integration with LocoForge

The NoSQL database integrates seamlessly with the LocoForge orchestrator:

- **Query Classification**: Automatically identifies NoSQL queries
- **Agent Routing**: Routes to NoSQL agent for supplies-related queries
- **Hybrid Queries**: Combines with SQL data for complex analysis
- **Error Handling**: Graceful fallback to Data Engineer agent

## File Structure

```
my_agent/utils/
├── setup_nosql_db.py          # Main setup script
├── nosql_agent.py             # NoSQL query executor
└── state.py                   # State management

test_nosql_setup.py            # Full integration tests
test_nosql_setup_mock.py       # Mock tests (no MongoDB)
demo_nosql_setup.py            # Demonstration script
```

## Contributing

To extend the data generation:

1. Add new product categories in `setup_nosql_db.py`
2. Extend store locations or purchase methods
3. Modify customer demographics or satisfaction patterns
4. Add new data generation methods

## License

This setup is part of the LocoForge project and follows the same licensing terms.
