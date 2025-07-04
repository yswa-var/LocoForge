# Deploying LocoForge to Render with SQLite3

## ⚠️ Important SQLite3 Limitations on Render

**SQLite3 has significant limitations when deployed to Render:**

1. **Ephemeral Storage**: Your SQLite database will be **reset on every deployment**
2. **No Data Persistence**: Data won't persist between deployments on free tier
3. **Concurrent Access Issues**: SQLite doesn't handle multiple concurrent writes well

## 🚀 Deployment Steps

### 1. Prepare Your Repository

```bash
git add .
git commit -m "Add Render deployment configuration"
git push origin main
```

### 2. Deploy to Render

1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click "New" → "Web Service"
3. Connect your GitHub repository
4. Configure:
   - **Name**: `locoforge-app`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app`
   - **Plan**: Free

### 3. Set Environment Variables

In Render dashboard, add:
```
OPENAPI_KEY=your_actual_openai_key
GOOGLE_API_KEY=your_actual_google_key
DATABASE_URL=sqlite:///employee_management.db
ENVIRONMENT=production
```

## 🔧 Testing Your Deployment

### Health Check
```bash
curl https://your-app-name.onrender.com/health
```

### Database Stats
```bash
curl https://your-app-name.onrender.com/api/database/stats
```

### Natural Language Query
```bash
curl -X POST https://your-app-name.onrender.com/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "How many employees are in Engineering?"}'
```

## 🚨 Important Notes

- **Data resets on every deployment** - SQLite database is recreated each time
- **Single worker** - Gunicorn configured for single worker due to SQLite limitations
- **Ephemeral storage** - Not suitable for production with persistent data needs

## 🔄 Alternative: PostgreSQL

For persistent data, consider PostgreSQL migration:

1. Add to requirements.txt: `psycopg2-binary>=2.9.0`
2. Create PostgreSQL service in Render
3. Update DATABASE_URL to use PostgreSQL connection string

## 📋 Current Deployment Setup

### Files Created:

1. **`render.yaml`** - Render service configuration
2. **`app.py`** - Flask web application
3. **`gunicorn.conf.py`** - Production server configuration
4. **`requirements.txt`** - Updated with Flask and Gunicorn

### Environment Variables Needed:

```bash
OPENAPI_KEY=your_OPENAPI_KEY
GOOGLE_API_KEY=your_google_api_key
DATABASE_URL=sqlite:///employee_management.db
ENVIRONMENT=production
```

## 🛠️ Deployment Steps

### 1. Prepare Your Repository

```bash
# Ensure all files are committed
git add .
git commit -m "Add Render deployment configuration"
git push origin main
```

### 2. Deploy to Render

#### Method A: Using render.yaml (Recommended)

1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click "New" → "Blueprint"
3. Connect your GitHub repository
4. Render will automatically detect `render.yaml`
5. Set your environment variables in the dashboard
6. Deploy

#### Method B: Manual Setup

1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click "New" → "Web Service"
3. Connect your GitHub repository
4. Configure:
   - **Name**: `locoforge-app`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app`
   - **Plan**: Free (or paid for persistent storage)

### 3. Set Environment Variables

In Render dashboard, add these environment variables:

```
OPENAPI_KEY=your_actual_openai_key
GOOGLE_API_KEY=your_actual_google_key
DATABASE_URL=sqlite:///employee_management.db
ENVIRONMENT=production
```

## 🔧 Testing Your Deployment

### Health Check
```bash
curl https://your-app-name.onrender.com/health
```

### Database Stats
```bash
curl https://your-app-name.onrender.com/api/database/stats
```

### Natural Language Query
```bash
curl -X POST https://your-app-name.onrender.com/api/query \
  -H "Content-Type: application/json" \
  -d '{"query": "How many employees are in the Engineering department?"}'
```

### Direct SQL Query
```bash
curl -X POST https://your-app-name.onrender.com/api/database/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM employees WHERE department_id = 1"}'
```

## 🚨 Troubleshooting

### Common Issues:

1. **Database not found**
   - Check if `sql_db_init.py` is being imported correctly
   - Verify database path in logs

2. **Agent initialization failed**
   - Check if all required environment variables are set
   - Verify API keys are valid

3. **Import errors**
   - Ensure all dependencies are in `requirements.txt`
   - Check Python version compatibility

### Debug Commands:

```bash
# Check application logs in Render dashboard
# Or use Render CLI if available

# Test locally before deploying
python app.py

# Check if database initializes correctly
python -c "from sql_db_ops.sql_db_init import create_employee_management_db; create_employee_management_db()"
```

## 🔄 Alternative: PostgreSQL Migration

If you need persistent data, consider migrating to PostgreSQL:

### 1. Add PostgreSQL to requirements.txt:
```
psycopg2-binary>=2.9.0
SQLAlchemy>=2.0.0
```

### 2. Create database abstraction layer:
```python
# database.py
from sqlalchemy import create_engine
import os

def get_database_url():
    if os.environ.get('DATABASE_URL'):
        return os.environ['DATABASE_URL']
    return 'sqlite:///employee_management.db'

engine = create_engine(get_database_url())
```

### 3. Update render.yaml:
```yaml
services:
  - type: web
    name: locoforge-app
    env: python
    plan: free
    buildCommand: pip install -r requirements.txt
    startCommand: gunicorn app:app
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: locoforge-db
          property: connectionString
  - type: pserv
    name: locoforge-db
    env: postgresql
    plan: free
```

## 📊 Monitoring and Logs

### Render Dashboard Features:
- **Logs**: Real-time application logs
- **Metrics**: CPU, memory usage
- **Deployments**: Deployment history and status
- **Environment Variables**: Secure storage of API keys

### Health Monitoring:
- Health check endpoint: `/health`
- Database connectivity monitoring
- Agent initialization status

## 🔒 Security Considerations

1. **API Keys**: Never commit API keys to repository
2. **Environment Variables**: Use Render's secure environment variable storage
3. **Database Access**: SQLite file is not exposed publicly
4. **Input Validation**: All user inputs are validated

## 📈 Scaling Considerations

### Free Tier Limitations:
- **Sleep after inactivity**: App sleeps after 15 minutes of inactivity
- **Cold starts**: First request after sleep takes longer
- **No persistent storage**: Data resets on deployments

### Paid Tier Benefits:
- **Always on**: No sleep mode
- **Persistent storage**: Data persists between deployments
- **Custom domains**: Use your own domain
- **SSL certificates**: Automatic HTTPS

## 🎯 Next Steps

1. **Deploy and test** the current SQLite setup
2. **Evaluate data persistence needs**
3. **Consider PostgreSQL migration** if persistent data is required
4. **Set up monitoring and alerts**
5. **Implement proper error handling and logging**

## 📞 Support

- **Render Documentation**: https://render.com/docs
- **Render Community**: https://community.render.com
- **SQLite Documentation**: https://www.sqlite.org/docs.html
- **Flask Documentation**: https://flask.palletsprojects.com/ 