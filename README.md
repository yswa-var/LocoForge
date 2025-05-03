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

# Google Drive Connector for LocoForge

## Setup Instructions

### 1. Create Google Cloud Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select existing one)
3. Enable the Google Drive API

### 2. Create Credentials
Choose one of these authentication methods:

#### Option A: OAuth Credentials (for user account access)
1. Go to "APIs & Services" > "Credentials"
2. Click "Create Credentials" > "OAuth client ID"
3. Choose "Desktop application"
4. Download the JSON file

#### Option B: Service Account (for automated/server use)
1. Go to "IAM & Admin" > "Service Accounts"
2. Create a service account
3. Create and download a JSON key for this service account
4. Share specific Drive folders/files with the service account email

### 3. Store Credentials Securely
```bash
# Create config directory
mkdir -p ~/.config/locoforge

# Move credentials to secure location
mv path/to/downloaded/credentials.json ~/.config/locoforge/oauth-credentials.json
# OR
mv path/to/downloaded/service-account.json ~/.config/locoforge/service-account.json

# Set proper permissions
chmod 600 ~/.config/locoforge/*.json
```

### 4. Create Configuration File
Create a file named `config.py`:

```python
import os

# Google Drive API credentials
GOOGLE_DRIVE_AUTH_METHOD = "oauth"  # or "service_account"
GOOGLE_DRIVE_CREDENTIALS_PATH = os.path.expanduser("~/.config/locoforge/oauth-credentials.json")
GOOGLE_DRIVE_TOKEN_PATH = os.path.expanduser("~/.config/locoforge/token.json")
```

### 5. Install Required Packages
```bash
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
```