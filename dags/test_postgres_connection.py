from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta


def test_postgres_connection():
    try:
        # Create a Postgres hook
        postgres_hook = PostgresHook(postgres_conn_id="postgres_con")

        # Test the connection
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        # Execute a simple query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("PostgreSQL Connection Test Successful!")
        print(f"PostgreSQL Version: {version[0]}")

        # Test if our tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE';
        """)
        tables = cursor.fetchall()
        print("\nAvailable tables:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"- {table[0]}: {count} records")

        cursor.close()
        connection.close()

    except Exception as e:
        print("PostgreSQL Connection Test Failed!")
        print(f"Error: {str(e)}")
        raise e


# Define DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    "test_postgres_connection",
    default_args=default_args,
    description="Test PostgreSQL Connection DAG",
    schedule_interval=None,
    catchup=False,
)

# Create task
test_connection = PythonOperator(
    task_id="test_postgres_connection",
    python_callable=test_postgres_connection,
    dag=dag,
)
