from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error

# --- SETUP ---
fake = Faker('id_ID')
Faker.seed(42)
random.seed(42)

NUM_CUSTOMERS = 5000
NUM_POSTS = 100
NUM_TRANSACTIONS = 100000
STATUS_OPTIONS = ['created', 'in_transit', 'delivered', 'done']

# --- MySQL Connection Configuration ---
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'airflow',
    'password': 'airflow',
    'database': 'airflow'
}

def create_mysql_connection():
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def create_tables(connection):
    try:
        cursor = connection.cursor()
        
        # Create customers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY,
                customer_name VARCHAR(100),
                customer_email VARCHAR(100),
                customer_phone VARCHAR(50),
                created_at DATETIME,
                updated_at DATETIME
            )
        """)
        
        # Create posts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                post_id INT PRIMARY KEY,
                post_origin VARCHAR(100),
                post_destination VARCHAR(100),
                created_at DATETIME,
                updated_at DATETIME
            )
        """)
        
        # Create transactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INT PRIMARY KEY,
                customer_id INT,
                post_id INT,
                last_status VARCHAR(20),
                created_at DATETIME,
                updated_at DATETIME,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                FOREIGN KEY (post_id) REFERENCES posts(post_id)
            )
        """)
        
        connection.commit()
        print("Tables created successfully")
    except Error as e:
        print(f"Error creating tables: {e}")

# --- Generate Data Functions ---
def generate_customers(n=NUM_CUSTOMERS):
    data = []
    for i in range(1, n + 1):
        created_at = fake.date_time_between(start_date='-6M', end_date='+1M')
        updated_at = created_at + timedelta(days=random.randint(0, 15))
        data.append({
            'customer_id': i,
            'customer_name': fake.name(),
            'customer_email': fake.email(),
            'customer_phone': fake.phone_number(),
            'created_at': created_at,
            'updated_at': updated_at
        })
    return pd.DataFrame(data)

def generate_posts(n=NUM_POSTS):
    data = []
    for i in range(1, n + 1):
        created_at = fake.date_time_between(start_date='-3M', end_date='+30d')
        updated_at = created_at + timedelta(days=random.randint(0, 5))
        data.append({
            'post_id': i,
            'post_origin': fake.city(),
            'post_destination': fake.city(),
            'created_at': created_at,
            'updated_at': updated_at
        })
    return pd.DataFrame(data)

def generate_transactions(customers, posts, n=NUM_TRANSACTIONS):
    data = []
    for i in range(1, n + 1):
        customer_id = random.choice(customers['customer_id'].tolist())
        post_id = random.choice(posts['post_id'].tolist())
        status = random.choice(STATUS_OPTIONS)
        created_at = fake.date_time_between(start_date='-30d', end_date='+30d')
        
        if status == 'created':
            updated_at = created_at
        else:
            updated_at = created_at + timedelta(hours=random.randint(1, 72))
            
        data.append({
            'id': i,
            'customer_id': customer_id,
            'post_id': post_id,
            'last_status': status,
            'created_at': created_at,
            'updated_at': updated_at
        })
    return pd.DataFrame(data)

def insert_data_to_mysql(connection, df, table_name):
    try:
        cursor = connection.cursor()
        
        # Convert dataframe to list of tuples
        values = df.values.tolist()
        # Create the placeholders for the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        # Create the column names string
        columns = ', '.join(df.columns)
        
        # Create the INSERT query
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Execute the query for each row
        cursor.executemany(query, values)
        connection.commit()
        print(f"Successfully inserted {len(values)} records into {table_name}")
        
    except Error as e:
        print(f"Error inserting data into {table_name}: {e}")

def main():
    # Create MySQL connection
    connection = create_mysql_connection()
    if not connection:
        return
    
    try:
        # Create tables
        create_tables(connection)
        
        # Generate data
        customers_df = generate_customers()
        posts_df = generate_posts()
        transactions_df = generate_transactions(customers_df, posts_df)
        
        # Ensure referential integrity
        valid_customer_ids = set(customers_df['customer_id'])
        valid_post_ids = set(posts_df['post_id'])
        transactions_df = transactions_df[
            transactions_df['customer_id'].isin(valid_customer_ids) & 
            transactions_df['post_id'].isin(valid_post_ids)
        ].reset_index(drop=True)
        
        # Insert data into MySQL
        insert_data_to_mysql(connection, customers_df, 'customers')
        insert_data_to_mysql(connection, posts_df, 'posts')
        insert_data_to_mysql(connection, transactions_df, 'transactions')
        
        print("✅ Data successfully generated and stored in MySQL!")
        
    except Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()