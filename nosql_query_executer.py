import pymongo
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid
import hashlib
from src.logger import setup_logger
import os

# Initialize logger
logger = setup_logger('nosql_query_executer')

# Initialize MongoDB client
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    # Force a connection to verify it works
    client.server_info()
    db = client["user_management_db"]
    logger.info("Successfully connected to MongoDB")
except pymongo.errors.ServerSelectionTimeoutError as e:
    logger.error(f"Could not connect to MongoDB server: {str(e)}")
    logger.error("Is MongoDB running on localhost:27017?")
    exit(1)

# Initialize Faker
fake = Faker()

# Clear existing collections
if os.environ.get('MONGODB_ENV') == 'development' or os.environ.get('ALLOW_DROP_COLLECTIONS') == 'true':
    logger.info("Dropping existing collections...")
    db.users.drop()
    db.roles.drop()
    db.activity_logs.drop()
else:
    logger.warning("Collection dropping disabled for safety. Set ALLOW_DROP_COLLECTIONS=true to enable.")
    user_input = input("Do you want to continue and drop collections? (y/N): ")
    if user_input.lower() == 'y':
        db.users.drop()
        db.roles.drop()
        db.activity_logs.drop()
        logger.info("Collections dropped based on user confirmation.")
    else:
        logger.info("Keeping existing collections.")
        
# Create collections
users_collection = db["users"]
roles_collection = db["roles"]
activity_logs_collection = db["activity_logs"]

def generate_password_hash(password):
    """Generate a simple hash for a password (for demo purposes only)"""
    return hashlib.sha256(password.encode()).hexdigest()

def generate_roles(num_roles=5):
    """Generate roles data and insert into MongoDB"""
    roles = [
        {
            "role_id": str(uuid.uuid4()),
            "name": "Admin",
            "description": "Full system access with all privileges",
            "permissions": ["create", "read", "update", "delete", "manage_users", "manage_roles"],
            "created_at": datetime.now() - timedelta(days=random.randint(100, 365))
        },
        {
            "role_id": str(uuid.uuid4()),
            "name": "Manager",
            "description": "Access to manage content and users",
            "permissions": ["create", "read", "update", "delete", "manage_users"],
            "created_at": datetime.now() - timedelta(days=random.randint(100, 365))
        },
        {
            "role_id": str(uuid.uuid4()),
            "name": "Editor",
            "description": "Can create and modify content",
            "permissions": ["create", "read", "update"],
            "created_at": datetime.now() - timedelta(days=random.randint(100, 365))
        },
        {
            "role_id": str(uuid.uuid4()),
            "name": "Viewer",
            "description": "Read-only access to content",
            "permissions": ["read"],
            "created_at": datetime.now() - timedelta(days=random.randint(100, 365))
        }
    ]
    
    # Add custom roles if needed
    while len(roles) < num_roles:
        role_name = fake.job().split()[0]  # Get first word of job title
        permissions = random.sample(["create", "read", "update", "delete"], random.randint(1, 4))
        
        role = {
            "role_id": str(uuid.uuid4()),
            "name": f"{role_name}",
            "description": f"{role_name} role with custom permissions",
            "permissions": permissions,
            "created_at": datetime.now() - timedelta(days=random.randint(30, 365))
        }
        roles.append(role)
    
    if roles:
        roles_collection.insert_many(roles)
        logger.info(f"Inserted {len(roles)} roles")
    
    return roles

def generate_users(roles, num_users=100):
    """Generate fake user data and insert into MongoDB"""
    users = []
    departments = ["Engineering", "Marketing", "Sales", "HR", "Finance", "Support", "Operations", "Research"]
    status_options = ["Active", "Inactive", "Suspended", "Pending"]
    
    for _ in range(num_users):
        # Choose a registration date
        registration_date = fake.date_time_between(start_date='-3y', end_date='now')
        
        # Choose a role
        role = random.choice(roles)
        
        # Generate last login time (could be None for new users)
        last_login = None
        if random.random() > 0.1:  # 90% of users have logged in
            last_login = fake.date_time_between(start_date=registration_date, end_date='now')
        
        # Generate a strong password
        raw_password = fake.password(length=12)
        
        user = {
            "user_id": str(uuid.uuid4()),
            "username": fake.user_name(),
            "email": fake.email(),
            "password_hash": generate_password_hash(raw_password),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "role_id": role["role_id"],
            "role_name": role["name"],
            "department": random.choice(departments),
            "phone": fake.phone_number(),
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "zip_code": fake.zipcode(),
                "country": fake.country()
            },
            "status": random.choice(status_options),
            "verified": random.random() > 0.1,  # 90% of users are verified
            "created_at": registration_date,
            "last_login": last_login,
            "profile": {
                "bio": fake.text(max_nb_chars=100) if random.random() > 0.3 else None,
                "profile_picture": f"https://randomuser.me/api/portraits/{random.choice(['men', 'women'])}/{random.randint(1, 99)}.jpg" if random.random() > 0.2 else None,
                "language_preference": random.choice(["en", "es", "fr", "de", "zh"])
            },
            "settings": {
                "notifications_enabled": random.random() > 0.2,
                "two_factor_auth": random.random() > 0.7,
                "theme": random.choice(["light", "dark", "system"])
            }
        }
        users.append(user)
    
    # Insert users in batches of 25 for better performance
    batch_size = 25
    for i in range(0, len(users), batch_size):
        batch = users[i:i+batch_size]
        if batch:
            users_collection.insert_many(batch)
            logger.info(f"Inserted batch of {len(batch)} users ({i+len(batch)}/{len(users)})")
    return users

def generate_activity_logs(users, num_logs=500):
    """Generate fake user activity logs and insert into MongoDB"""
    activity_logs = []
    activity_types = [
        "login", "logout", "profile_update", "password_change", 
        "failed_login", "file_download", "file_upload", "settings_change",
        "permission_granted", "permission_revoked", "account_locked"
    ]
    
    for _ in range(num_logs):
        user = random.choice(users)
        activity_type = random.choice(activity_types)
        
        # Ensure activity timestamp is after user creation
        activity_time = fake.date_time_between(
            start_date=user["created_at"],
            end_date='now'
        )
        
        # Generate appropriate details based on activity type
        details = {}
        if activity_type == "login":
            details = {
                "ip_address": fake.ipv4(),
                "device": random.choice(["desktop", "mobile", "tablet"]),
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "success": True
            }
        elif activity_type == "failed_login":
            details = {
                "ip_address": fake.ipv4(),
                "device": random.choice(["desktop", "mobile", "tablet"]),
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "reason": random.choice(["invalid_password", "account_locked", "security_check_failed"])
            }
        elif activity_type == "profile_update":
            details = {
                "fields_changed": random.sample(["name", "email", "phone", "address", "bio"], 
                                             random.randint(1, 3))
            }
        elif activity_type == "settings_change":
            details = {
                "setting": random.choice(["notifications", "privacy", "theme", "language"]),
                "old_value": "old_setting_value",
                "new_value": "new_setting_value"
            }
        
        activity_log = {
            "log_id": str(uuid.uuid4()),
            "user_id": user["user_id"],
            "username": user["username"],
            "activity_type": activity_type,
            "timestamp": activity_time,
            "details": details,
            "ip_address": fake.ipv4() if "ip_address" not in details else details["ip_address"]
        }
        activity_logs.append(activity_log)
    
    if activity_logs:
        activity_logs_collection.insert_many(activity_logs)
        logger.info(f"Inserted {len(activity_logs)} activity logs")

def main():
    """Generate all fake data for the user management database"""
    # Generate data
    logger.info("Generating fake user management data for MongoDB...")
    roles = generate_roles(num_roles=7)
    users = generate_users(roles, num_users=100)
    generate_activity_logs(users, num_logs=500)
    
    # Print some sample documents
    logger.info("\nSample role document:")
    logger.info(str(roles_collection.find_one()))
    
    logger.info("\nSample user document:")
    logger.info(str(users_collection.find_one()))
    
    logger.info("\nSample activity log document:")
    logger.info(str(activity_logs_collection.find_one()))
    
    logger.info("\nDone! MongoDB collections created with fake user management data.")

if __name__ == "__main__":
    main()