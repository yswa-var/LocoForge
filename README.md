# LocoForge

## LLM-based DB Connector


### Setting Up Your Environment

1. **Create a Virtual Environment**

   On Windows:
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```

   On macOS/Linux:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install Requirements**
   ```bash
   pip install -r requirements.txt
   ```

3. **MongoDB Setup**
   
   Before running the NoSQL database operations, ensure MongoDB is installed and running:

   - **Install MongoDB**:
     - On macOS (using Homebrew):
       ```bash
       brew tap mongodb/brew
       brew install mongodb-community
       ```
     - On Windows: Download and install from [MongoDB Community Server](https://www.mongodb.com/try/download/community)
     - On Linux (Ubuntu):
       ```bash
       sudo apt-get update
       sudo apt-get install mongodb
       ```

   - **Start MongoDB Service**:
     - On macOS:
       ```bash
       brew services start mongodb-community
       ```
     - On Windows: MongoDB runs as a service automatically after installation
     - On Linux:
       ```bash
       sudo systemctl start mongodb
       ```

   - **Verify MongoDB is Running**:
     ```bash
     mongosh
     ```
     If you see the MongoDB shell, the service is running correctly.

### Running the Database Operations

Execute the `db_ops.py` file to manage the SQLite database:

```bash
python db_ops.py
```

The script performs the following operations:
- Checks if a database named `ohlc.db` exists in the current directory
- If it exists, connects to it and verifies the `ohlc` table
- If the database or table doesn't exist, creates them and populates the table with Apple (AAPL) stock data from January 2020 to the present
- Displays the latest 5 records from the database

### Running NoSQL Database Operations

Execute the `db_ops_nosql.py` file to manage the MongoDB database:

```bash
python db_ops_nosql.py
```

The script performs similar operations as the SQLite version but using MongoDB:
- Connects to MongoDB running on localhost:27017
- Checks if the `stock_data` database and `ohlc` collection exist
- If they don't exist, creates them and populates with Apple (AAPL) stock data
- Displays the latest 5 records from the collection

