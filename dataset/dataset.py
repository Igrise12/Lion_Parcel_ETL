from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

# --- SETUP ---
fake = Faker('id_ID')  # lokal Indonesia biar datanya natural
Faker.seed(42)
random.seed(42)

NUM_CUSTOMERS = 10
NUM_POSTS = 15
NUM_TRANSACTIONS = 30
STATUS_OPTIONS = ['created', 'in_transit', 'delivered', 'done']

# --- 1. Generate Customers Table ---
def generate_customers(n=NUM_CUSTOMERS):
    data = []
    for i in range(1, n + 1):
        created_at = fake.date_time_between(start_date='-6M', end_date='-1M')
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

# --- 2. Generate Posts Table ---
def generate_posts(n=NUM_POSTS):
    data = []
    for i in range(1, n + 1):
        created_at = fake.date_time_between(start_date='-3M', end_date='-10d')
        updated_at = created_at + timedelta(days=random.randint(0, 5))
        data.append({
            'post_id': i,
            'post_origin': fake.city(),
            'post_destination': fake.city(),
            'created_at': created_at,
            'updated_at': updated_at
        })
    return pd.DataFrame(data)

# --- 3. Generate Transactions Table ---
def generate_transactions(customers, posts, n=NUM_TRANSACTIONS):
    data = []
    for i in range(1, n + 1):
        customer_id = random.choice(customers['customer_id'].tolist())
        post_id = random.choice(posts['post_id'].tolist())
        status = random.choice(STATUS_OPTIONS)

        created_at = fake.date_time_between(start_date='-30d', end_date='-1d')

        # aturan waktu
        if status == 'created':
            updated_at = created_at
        else:
            updated_at = created_at + timedelta(hours=random.randint(1, 72))  # 1–3 hari setelahnya

        data.append({
            'id': i,
            'customer_id': customer_id,
            'post_id': post_id,
            'last_status': status,
            'created_at': created_at,
            'updated_at': updated_at
        })
    return pd.DataFrame(data)

# --- MAIN EXECUTION ---
customers_df = generate_customers()
posts_df = generate_posts()
transactions_df = generate_transactions(customers_df, posts_df)

# --- VALIDATION: Ensure Referential Integrity ---
valid_customer_ids = set(customers_df['customer_id'])
valid_post_ids = set(posts_df['post_id'])
transactions_df = transactions_df[
    transactions_df['customer_id'].isin(valid_customer_ids) & 
    transactions_df['post_id'].isin(valid_post_ids)
].reset_index(drop=True)

# --- OUTPUT ---
print("=== Customers ===")
print(customers_df.head(), "\n")
print("=== Posts ===")
print(posts_df.head(), "\n")
print("=== Transactions ===")
print(transactions_df.head(), "\n")

# --- SAVE TO CSV ---
customers_df.to_csv('dataset/datasets/customers.csv', index=False)
posts_df.to_csv('dataset/datasets/posts.csv', index=False)
transactions_df.to_csv('dataset/datasets/transactions.csv', index=False)

print("✅ Data berhasil digenerate dan disimpan ke CSV!")
