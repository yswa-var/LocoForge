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

