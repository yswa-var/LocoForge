import sqlite3
import os
from datetime import datetime, date

def create_employee_management_db():
    """Create SQLite database with employee management tables and mock data"""
    
    # Create database file
    db_path = "/Users/apple/lgstudioSetup/LocoForge/employee_management.db"
    
    # Remove existing database if it exists
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    create_tables(cursor)
    
    # Insert mock data
    insert_mock_data(cursor)
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print(f"Database '{db_path}' created successfully with mock data!")
    return db_path

def create_tables(cursor):
    """Create all tables for the employee management system"""
    
    # 1. Departments table
    cursor.execute('''
        CREATE TABLE departments (
            department_id INTEGER PRIMARY KEY AUTOINCREMENT,
            department_name TEXT NOT NULL UNIQUE,
            location TEXT NOT NULL,
            budget REAL DEFAULT 0.0,
            created_date DATE DEFAULT CURRENT_DATE
        )
    ''')
    
    # 2. Employees table
    cursor.execute('''
        CREATE TABLE employees (
            employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT,
            hire_date DATE NOT NULL,
            salary REAL NOT NULL,
            department_id INTEGER,
            manager_id INTEGER,
            position TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            FOREIGN KEY (department_id) REFERENCES departments (department_id),
            FOREIGN KEY (manager_id) REFERENCES employees (employee_id)
        )
    ''')
    
    # 3. Projects table
    cursor.execute('''
        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT NOT NULL,
            description TEXT,
            start_date DATE NOT NULL,
            end_date DATE,
            budget REAL DEFAULT 0.0,
            status TEXT DEFAULT 'active',
            department_id INTEGER,
            project_manager_id INTEGER,
            FOREIGN KEY (department_id) REFERENCES departments (department_id),
            FOREIGN KEY (project_manager_id) REFERENCES employees (employee_id)
        )
    ''')
    
    # 4. Employee_Projects (Many-to-Many relationship)
    cursor.execute('''
        CREATE TABLE employee_projects (
            assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            hours_allocated INTEGER DEFAULT 0,
            start_date DATE NOT NULL,
            end_date DATE,
            FOREIGN KEY (employee_id) REFERENCES employees (employee_id),
            FOREIGN KEY (project_id) REFERENCES projects (project_id),
            UNIQUE(employee_id, project_id)
        )
    ''')
    
    # 5. Attendance table
    cursor.execute('''
        CREATE TABLE attendance (
            attendance_id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id INTEGER NOT NULL,
            date DATE NOT NULL,
            check_in_time TIME,
            check_out_time TIME,
            hours_worked REAL DEFAULT 0.0,
            status TEXT DEFAULT 'present',
            FOREIGN KEY (employee_id) REFERENCES employees (employee_id),
            UNIQUE(employee_id, date)
        )
    ''')

def insert_mock_data(cursor):
    """Insert comprehensive mock data into all tables"""
    
    # Insert departments with varied budgets for aggregation queries
    departments_data = [
        ('Engineering', 'Floor 3, Building A', 850000.0),
        ('Marketing', 'Floor 2, Building A', 450000.0),
        ('Sales', 'Floor 1, Building A', 600000.0),
        ('Human Resources', 'Floor 2, Building B', 250000.0),
        ('Finance', 'Floor 1, Building B', 400000.0),
        ('Operations', 'Floor 4, Building A', 550000.0),
        ('IT Support', 'Floor 3, Building B', 350000.0),
        ('Research & Development', 'Floor 5, Building A', 1200000.0),
        ('Customer Service', 'Floor 1, Building C', 300000.0),
        ('Legal', 'Floor 2, Building C', 200000.0)
    ]
    
    cursor.executemany('''
        INSERT INTO departments (department_name, location, budget)
        VALUES (?, ?, ?)
    ''', departments_data)
    
    # Insert employees with varied salaries for aggregation queries
    employees_data = [
        # Engineering Department (8 employees)
        ('John', 'Smith', 'john.smith@company.com', '555-0101', '2020-01-15', 95000.0, 1, None, 'Senior Software Engineer'),
        ('Sarah', 'Johnson', 'sarah.johnson@company.com', '555-0102', '2021-03-20', 85000.0, 1, 1, 'Software Engineer'),
        ('Mike', 'Davis', 'mike.davis@company.com', '555-0103', '2022-06-10', 75000.0, 1, 1, 'Junior Developer'),
        ('Emily', 'Wilson', 'emily.wilson@company.com', '555-0104', '2021-08-15', 80000.0, 1, 1, 'QA Engineer'),
        ('Alex', 'Chen', 'alex.chen@company.com', '555-0105', '2023-01-10', 70000.0, 1, 1, 'Frontend Developer'),
        ('Jessica', 'Brown', 'jessica.brown@company.com', '555-0106', '2022-09-15', 78000.0, 1, 1, 'Backend Developer'),
        ('David', 'Miller', 'david.miller@company.com', '555-0107', '2023-03-20', 65000.0, 1, 1, 'DevOps Engineer'),
        ('Lisa', 'Garcia', 'lisa.garcia@company.com', '555-0108', '2021-12-01', 82000.0, 1, 1, 'System Architect'),
        
        # Marketing Department (6 employees)
        ('Robert', 'Taylor', 'robert.taylor@company.com', '555-0201', '2019-11-05', 82000.0, 2, None, 'Marketing Manager'),
        ('Amanda', 'Anderson', 'amanda.anderson@company.com', '555-0202', '2022-01-10', 65000.0, 2, 9, 'Marketing Specialist'),
        ('Chris', 'Martinez', 'chris.martinez@company.com', '555-0203', '2023-02-28', 58000.0, 2, 9, 'Content Creator'),
        ('Jennifer', 'Lee', 'jennifer.lee@company.com', '555-0204', '2022-07-15', 62000.0, 2, 9, 'Social Media Manager'),
        ('Michael', 'White', 'michael.white@company.com', '555-0205', '2023-05-10', 55000.0, 2, 9, 'Digital Marketing Specialist'),
        ('Rachel', 'Clark', 'rachel.clark@company.com', '555-0206', '2021-10-20', 68000.0, 2, 9, 'Brand Manager'),
        
        # Sales Department (7 employees)
        ('Patricia', 'Robinson', 'patricia.robinson@company.com', '555-0301', '2018-05-12', 90000.0, 3, None, 'Sales Director'),
        ('Kevin', 'Clark', 'kevin.clark@company.com', '555-0302', '2021-09-20', 70000.0, 3, 15, 'Sales Representative'),
        ('Maria', 'Rodriguez', 'maria.rodriguez@company.com', '555-0303', '2022-12-01', 68000.0, 3, 15, 'Sales Representative'),
        ('James', 'Lewis', 'james.lewis@company.com', '555-0304', '2023-04-15', 62000.0, 3, 15, 'Sales Representative'),
        ('Thomas', 'Walker', 'thomas.walker@company.com', '555-0305', '2022-08-30', 65000.0, 3, 15, 'Account Manager'),
        ('Linda', 'Hall', 'linda.hall@company.com', '555-0306', '2023-01-20', 58000.0, 3, 15, 'Sales Representative'),
        ('Daniel', 'Young', 'daniel.young@company.com', '555-0307', '2021-11-10', 72000.0, 3, 15, 'Senior Sales Representative'),
        
        # HR Department (5 employees)
        ('Elizabeth', 'Allen', 'elizabeth.allen@company.com', '555-0401', '2020-02-14', 78000.0, 4, None, 'HR Manager'),
        ('Christopher', 'King', 'christopher.king@company.com', '555-0402', '2023-04-15', 62000.0, 4, 22, 'HR Specialist'),
        ('Michelle', 'Wright', 'michelle.wright@company.com', '555-0403', '2022-06-20', 58000.0, 4, 22, 'Recruiter'),
        ('Steven', 'Lopez', 'steven.lopez@company.com', '555-0404', '2023-08-10', 55000.0, 4, 22, 'HR Coordinator'),
        ('Nicole', 'Hill', 'nicole.hill@company.com', '555-0405', '2021-12-05', 65000.0, 4, 22, 'Benefits Specialist'),
        
        # Finance Department (6 employees)
        ('Andrew', 'Scott', 'andrew.scott@company.com', '555-0501', '2019-07-22', 85000.0, 5, None, 'Finance Manager'),
        ('Samantha', 'Green', 'samantha.green@company.com', '555-0502', '2022-08-30', 72000.0, 5, 27, 'Accountant'),
        ('Ryan', 'Baker', 'ryan.baker@company.com', '555-0503', '2023-03-15', 68000.0, 5, 27, 'Financial Analyst'),
        ('Stephanie', 'Adams', 'stephanie.adams@company.com', '555-0504', '2022-11-20', 65000.0, 5, 27, 'Accountant'),
        ('Brandon', 'Nelson', 'brandon.nelson@company.com', '555-0505', '2023-07-01', 60000.0, 5, 27, 'Payroll Specialist'),
        ('Melissa', 'Carter', 'melissa.carter@company.com', '555-0506', '2021-09-10', 75000.0, 5, 27, 'Senior Accountant'),
        
        # Operations Department (5 employees)
        ('Joshua', 'Mitchell', 'joshua.mitchell@company.com', '555-0601', '2021-01-10', 88000.0, 6, None, 'Operations Manager'),
        ('Ashley', 'Perez', 'ashley.perez@company.com', '555-0602', '2023-03-15', 64000.0, 6, 33, 'Operations Coordinator'),
        ('Matthew', 'Roberts', 'matthew.roberts@company.com', '555-0603', '2022-05-20', 62000.0, 6, 33, 'Logistics Specialist'),
        ('Amber', 'Turner', 'amber.turner@company.com', '555-0604', '2023-09-05', 58000.0, 6, 33, 'Operations Assistant'),
        ('Justin', 'Phillips', 'justin.phillips@company.com', '555-0605', '2022-12-10', 66000.0, 6, 33, 'Supply Chain Coordinator'),
        
        # IT Support Department (4 employees)
        ('Lauren', 'Campbell', 'lauren.campbell@company.com', '555-0701', '2020-08-15', 75000.0, 7, None, 'IT Support Manager'),
        ('Tyler', 'Parker', 'tyler.parker@company.com', '555-0702', '2022-10-20', 58000.0, 7, 38, 'IT Support Specialist'),
        ('Brittany', 'Evans', 'brittany.evans@company.com', '555-0703', '2023-06-15', 55000.0, 7, 38, 'Help Desk Technician'),
        ('Zachary', 'Edwards', 'zachary.edwards@company.com', '555-0704', '2022-04-10', 62000.0, 7, 38, 'Network Administrator'),
        
        # Research & Development Department (6 employees)
        ('Rebecca', 'Collins', 'rebecca.collins@company.com', '555-0801', '2018-12-01', 120000.0, 8, None, 'R&D Director'),
        ('Nathan', 'Stewart', 'nathan.stewart@company.com', '555-0802', '2021-05-15', 95000.0, 8, 42, 'Senior Research Scientist'),
        ('Hannah', 'Sanchez', 'hannah.sanchez@company.com', '555-0803', '2022-09-20', 88000.0, 8, 42, 'Research Scientist'),
        ('Dylan', 'Morris', 'dylan.morris@company.com', '555-0804', '2023-01-10', 82000.0, 8, 42, 'Data Scientist'),
        ('Victoria', 'Rogers', 'victoria.rogers@company.com', '555-0805', '2022-07-05', 90000.0, 8, 42, 'Senior Data Analyst'),
        ('Caleb', 'Reed', 'caleb.reed@company.com', '555-0806', '2023-04-20', 78000.0, 8, 42, 'Research Assistant'),
        
        # Customer Service Department (4 employees)
        ('Alyssa', 'Cook', 'alyssa.cook@company.com', '555-0901', '2021-03-10', 65000.0, 9, None, 'Customer Service Manager'),
        ('Ethan', 'Morgan', 'ethan.morgan@company.com', '555-0902', '2022-11-15', 52000.0, 9, 48, 'Customer Service Representative'),
        ('Madison', 'Bell', 'madison.bell@company.com', '555-0903', '2023-08-20', 50000.0, 9, 48, 'Customer Service Representative'),
        ('Noah', 'Murphy', 'noah.murphy@company.com', '555-0904', '2022-12-05', 54000.0, 9, 48, 'Customer Service Specialist'),
        
        # Legal Department (3 employees)
        ('Chloe', 'Bailey', 'chloe.bailey@company.com', '555-1001', '2019-06-20', 110000.0, 10, None, 'Legal Counsel'),
        ('Mason', 'Rivera', 'mason.rivera@company.com', '555-1002', '2022-03-15', 85000.0, 10, 52, 'Legal Assistant'),
        ('Lily', 'Cooper', 'lily.cooper@company.com', '555-1003', '2023-10-01', 75000.0, 10, 52, 'Paralegal')
    ]
    
    cursor.executemany('''
        INSERT INTO employees (first_name, last_name, email, phone, hire_date, salary, department_id, manager_id, position)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', employees_data)
    
    # Insert projects
    projects_data = [
        ('Website Redesign', 'Complete overhaul of company website', '2024-01-15', '2024-06-30', 150000.0, 'active', 1, 1),
        ('Mobile App Development', 'New mobile application for customers', '2024-02-01', '2024-08-31', 200000.0, 'active', 1, 1),
        ('Q4 Marketing Campaign', 'Holiday season marketing campaign', '2024-10-01', '2024-12-31', 75000.0, 'planned', 2, 9),
        ('Sales Training Program', 'Comprehensive sales training for new hires', '2024-03-01', '2024-05-31', 25000.0, 'active', 3, 15),
        ('HR System Implementation', 'New HR management system', '2024-04-01', '2024-09-30', 100000.0, 'active', 4, 22),
        ('Financial Audit 2024', 'Annual financial audit preparation', '2024-11-01', '2024-12-31', 50000.0, 'planned', 5, 27),
        ('Office Relocation', 'Moving to new office space', '2024-07-01', '2024-08-31', 75000.0, 'planned', 6, 33),
        ('IT Infrastructure Upgrade', 'Upgrading company IT systems', '2024-05-01', '2024-10-31', 120000.0, 'active', 7, 38),
        ('AI Research Project', 'Developing AI-powered solutions', '2024-03-01', '2024-12-31', 300000.0, 'active', 8, 42),
        ('Customer Portal Development', 'New customer self-service portal', '2024-06-01', '2024-11-30', 80000.0, 'planned', 9, 48),
        ('Legal Compliance Review', 'Annual legal compliance audit', '2024-09-01', '2024-12-31', 40000.0, 'planned', 10, 52)
    ]
    
    cursor.executemany('''
        INSERT INTO projects (project_name, description, start_date, end_date, budget, status, department_id, project_manager_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', projects_data)
    
    # Insert employee-project assignments
    assignments_data = [
        # Website Redesign Project
        (1, 1, 'Lead Developer', 40, '2024-01-15', '2024-06-30'),
        (2, 1, 'Frontend Developer', 35, '2024-01-15', '2024-06-30'),
        (3, 1, 'Backend Developer', 30, '2024-01-15', '2024-06-30'),
        (4, 1, 'QA Tester', 25, '2024-02-01', '2024-06-30'),
        (5, 1, 'UI/UX Designer', 30, '2024-01-15', '2024-06-30'),
        
        # Mobile App Development Project
        (1, 2, 'Project Manager', 20, '2024-02-01', '2024-08-31'),
        (2, 2, 'Mobile Developer', 40, '2024-02-01', '2024-08-31'),
        (3, 2, 'UI/UX Designer', 30, '2024-02-01', '2024-08-31'),
        (6, 2, 'Backend Developer', 35, '2024-02-01', '2024-08-31'),
        (7, 2, 'DevOps Engineer', 25, '2024-02-01', '2024-08-31'),
        
        # Q4 Marketing Campaign
        (9, 3, 'Campaign Manager', 40, '2024-10-01', '2024-12-31'),
        (10, 3, 'Content Creator', 35, '2024-10-01', '2024-12-31'),
        (11, 3, 'Social Media Specialist', 25, '2024-10-01', '2024-12-31'),
        (12, 3, 'Brand Manager', 30, '2024-10-01', '2024-12-31'),
        (13, 3, 'Digital Marketing Specialist', 35, '2024-10-01', '2024-12-31'),
        
        # Sales Training Program
        (15, 4, 'Training Coordinator', 40, '2024-03-01', '2024-05-31'),
        (16, 4, 'Sales Trainer', 30, '2024-03-01', '2024-05-31'),
        (17, 4, 'Training Assistant', 20, '2024-03-01', '2024-05-31'),
        (18, 4, 'Account Manager', 25, '2024-03-01', '2024-05-31'),
        (19, 4, 'Sales Representative', 20, '2024-03-01', '2024-05-31'),
        
        # HR System Implementation
        (22, 5, 'Project Manager', 40, '2024-04-01', '2024-09-30'),
        (23, 5, 'System Analyst', 35, '2024-04-01', '2024-09-30'),
        (24, 5, 'Recruiter', 25, '2024-04-01', '2024-09-30'),
        (25, 5, 'HR Coordinator', 30, '2024-04-01', '2024-09-30'),
        (26, 5, 'Benefits Specialist', 25, '2024-04-01', '2024-09-30'),
        
        # Financial Audit 2024
        (27, 6, 'Audit Lead', 40, '2024-11-01', '2024-12-31'),
        (28, 6, 'Accountant', 35, '2024-11-01', '2024-12-31'),
        (29, 6, 'Financial Analyst', 30, '2024-11-01', '2024-12-31'),
        (30, 6, 'Accountant', 35, '2024-11-01', '2024-12-31'),
        (31, 6, 'Payroll Specialist', 25, '2024-11-01', '2024-12-31'),
        
        # Office Relocation
        (33, 7, 'Project Manager', 40, '2024-07-01', '2024-08-31'),
        (34, 7, 'Logistics Coordinator', 35, '2024-07-01', '2024-08-31'),
        (35, 7, 'Operations Assistant', 30, '2024-07-01', '2024-08-31'),
        (36, 7, 'Supply Chain Coordinator', 35, '2024-07-01', '2024-08-31'),
        
        # IT Infrastructure Upgrade
        (38, 8, 'Project Manager', 40, '2024-05-01', '2024-10-31'),
        (39, 8, 'Network Administrator', 35, '2024-05-01', '2024-10-31'),
        (40, 8, 'IT Support Specialist', 30, '2024-05-01', '2024-10-31'),
        (41, 8, 'Help Desk Technician', 25, '2024-05-01', '2024-10-31'),
        
        # AI Research Project
        (42, 9, 'Project Director', 40, '2024-03-01', '2024-12-31'),
        (43, 9, 'Senior Research Scientist', 40, '2024-03-01', '2024-12-31'),
        (44, 9, 'Research Scientist', 35, '2024-03-01', '2024-12-31'),
        (45, 9, 'Data Scientist', 40, '2024-03-01', '2024-12-31'),
        (46, 9, 'Senior Data Analyst', 35, '2024-03-01', '2024-12-31'),
        (47, 9, 'Research Assistant', 30, '2024-03-01', '2024-12-31'),
        
        # Customer Portal Development
        (48, 10, 'Project Manager', 40, '2024-06-01', '2024-11-30'),
        (49, 10, 'Customer Service Representative', 30, '2024-06-01', '2024-11-30'),
        (50, 10, 'Customer Service Representative', 30, '2024-06-01', '2024-11-30'),
        (51, 10, 'Customer Service Specialist', 35, '2024-06-01', '2024-11-30'),
        
        # Legal Compliance Review
        (52, 11, 'Project Lead', 40, '2024-09-01', '2024-12-31'),
        (53, 11, 'Legal Assistant', 35, '2024-09-01', '2024-12-31'),
        (54, 11, 'Paralegal', 30, '2024-09-01', '2024-12-31')
    ]
    
    cursor.executemany('''
        INSERT INTO employee_projects (employee_id, project_id, role, hours_allocated, start_date, end_date)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', assignments_data)
    
    # Insert comprehensive attendance records (last 60 days for better statistics)
    import random
    from datetime import timedelta
    
    attendance_data = []
    base_date = date.today() - timedelta(days=60)
    
    for employee_id in range(1, 55):  # All employees
        for day in range(60):
            current_date = base_date + timedelta(days=day)
            
            # Skip weekends
            if current_date.weekday() >= 5:
                continue
                
            # Random attendance patterns with varied rates by department
            attendance_rate = 0.92  # Base 92% attendance rate
            
            # Adjust attendance rate based on department (for realistic variation)
            if employee_id <= 8:  # Engineering - high attendance
                attendance_rate = 0.95
            elif employee_id <= 14:  # Marketing - good attendance
                attendance_rate = 0.93
            elif employee_id <= 21:  # Sales - variable attendance
                attendance_rate = 0.90
            elif employee_id <= 26:  # HR - good attendance
                attendance_rate = 0.94
            elif employee_id <= 32:  # Finance - excellent attendance
                attendance_rate = 0.96
            elif employee_id <= 37:  # Operations - good attendance
                attendance_rate = 0.93
            elif employee_id <= 41:  # IT Support - good attendance
                attendance_rate = 0.92
            elif employee_id <= 47:  # R&D - excellent attendance
                attendance_rate = 0.95
            elif employee_id <= 51:  # Customer Service - variable attendance
                attendance_rate = 0.88
            else:  # Legal - excellent attendance
                attendance_rate = 0.97
            
            if random.random() < attendance_rate:
                # Normal work day
                check_in = f"{random.randint(8, 9):02d}:{random.randint(0, 59):02d}"
                check_out = f"{random.randint(17, 18):02d}:{random.randint(0, 59):02d}"
                hours_worked = random.uniform(7.5, 9.0)
                status = 'present'
            else:
                # Absent day
                check_in = None
                check_out = None
                hours_worked = 0.0
                status = 'absent'
            
            attendance_data.append((
                employee_id, 
                current_date.strftime('%Y-%m-%d'),
                check_in,
                check_out,
                round(hours_worked, 2),
                status
            ))
    
    cursor.executemany('''
        INSERT INTO attendance (employee_id, date, check_in_time, check_out_time, hours_worked, status)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', attendance_data)

def print_database_info():
    """Print information about the created database"""
    conn = sqlite3.connect("employee_management.db")
    cursor = conn.cursor()
    
    print("\n=== EMPLOYEE MANAGEMENT DATABASE INFO ===")
    
    # Get table information
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Table '{table_name}': {count} records")
    
    # Sample queries to verify data
    print("\n=== SAMPLE DATA ===")
    
    # Sample employees by department
    cursor.execute("""
        SELECT d.department_name, COUNT(e.employee_id) as employee_count
        FROM departments d 
        LEFT JOIN employees e ON d.department_id = e.department_id 
        GROUP BY d.department_id, d.department_name
        ORDER BY employee_count DESC
    """)
    dept_counts = cursor.fetchall()
    print("Employees by Department:")
    for dept in dept_counts:
        print(f"  {dept[0]}: {dept[1]} employees")
    
    # Sample salary statistics
    cursor.execute("""
        SELECT 
            d.department_name,
            COUNT(e.employee_id) as emp_count,
            AVG(e.salary) as avg_salary,
            MIN(e.salary) as min_salary,
            MAX(e.salary) as max_salary
        FROM departments d 
        LEFT JOIN employees e ON d.department_id = e.department_id 
        GROUP BY d.department_id, d.department_name
        ORDER BY avg_salary DESC
    """)
    salary_stats = cursor.fetchall()
    print("\nSalary Statistics by Department:")
    for stat in salary_stats:
        print(f"  {stat[0]}: {stat[1]} employees, Avg: ${stat[2]:,.0f}, Range: ${stat[3]:,.0f}-${stat[4]:,.0f}")
    
    # Total budget across all departments
    cursor.execute("SELECT SUM(budget) FROM departments")
    total_budget = cursor.fetchone()[0]
    print(f"\nTotal Budget Across All Departments: ${total_budget:,.0f}")
    
    # Total employee salaries
    cursor.execute("SELECT SUM(salary) FROM employees")
    total_salaries = cursor.fetchone()[0]
    print(f"Total Employee Salaries: ${total_salaries:,.0f}")
    
    # Highest paid employee
    cursor.execute("""
        SELECT e.first_name, e.last_name, e.salary, d.department_name, e.position
        FROM employees e 
        JOIN departments d ON e.department_id = d.department_id 
        ORDER BY e.salary DESC 
        LIMIT 1
    """)
    highest_paid = cursor.fetchone()
    print(f"Highest Paid Employee: {highest_paid[0]} {highest_paid[1]} - ${highest_paid[2]:,.0f} ({highest_paid[3]} - {highest_paid[4]})")
    
    # Department with lowest budget
    cursor.execute("""
        SELECT department_name, budget 
        FROM departments 
        ORDER BY budget ASC 
        LIMIT 1
    """)
    lowest_budget = cursor.fetchone()
    print(f"Department with Lowest Budget: {lowest_budget[0]} - ${lowest_budget[1]:,.0f}")
    
    # Average attendance rate
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN status = 'present' THEN 1 END) * 100.0 / COUNT(*) as attendance_rate
        FROM attendance
    """)
    attendance_rate = cursor.fetchone()[0]
    print(f"Overall Attendance Rate: {attendance_rate:.1f}%")
    
    conn.close()

if __name__ == "__main__":
    # Create the database
    db_path = create_employee_management_db()
    
    # Print database information
    print_database_info()
    
    print(f"\nDatabase file created at: {os.path.abspath(db_path)}")
    print("You can now use this database for testing your SQL agent!")
    print("\nThis database now supports all aggregation queries including:")
    print("- Count queries: employees by department, employees by position")
    print("- Sum queries: total budget, total salaries")
    print("- Average queries: average salary by department, attendance rates")
    print("- Min/Max queries: highest paid employee, lowest budget department")
