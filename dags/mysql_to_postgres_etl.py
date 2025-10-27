from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import logging

# Set up logging
logger = logging.getLogger(__name__)


def test_connections():
    """Test both MySQL and PostgreSQL connections"""
    try:
        # Test MySQL connection
        mysql_hook = MySqlHook(mysql_conn_id="airflow_con")
        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute("SELECT 1")
        logger.info("MySQL connection test successful!")
        mysql_cursor.close()

        # Test PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id="postgres_con")
        pg_conn = pg_hook.get_conn()
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute("SELECT 1")
        logger.info("PostgreSQL connection test successful!")
        pg_cursor.close()

        return True
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        raise


def create_target_table():
    """Create the target table in PostgreSQL if it doesn't exist"""
    pg_hook = PostgresHook(postgres_conn_id="postgres_con")

    # Check if table exists
    check_table_sql = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'shipping_fact'
    );
    """
    table_exists = pg_hook.get_first(check_table_sql)[0]

    if not table_exists:
        create_table_sql = """
        CREATE TABLE shipping_fact (
            id SERIAL PRIMARY KEY,
            receipt_id INTEGER UNIQUE NOT NULL,
            customer_id INTEGER,
            last_status VARCHAR(20),
            pos_origin VARCHAR(100),
            pos_destination VARCHAR(100),
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            deleted_at TIMESTAMP,
            etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create index for better query performance
        CREATE INDEX idx_shipping_customer_id ON shipping_fact(customer_id);
        CREATE INDEX idx_shipping_status ON shipping_fact(last_status);
        """

        pg_hook.run(create_table_sql)
        logger.info("Target table created successfully!")
    else:
        logger.info("Target table already exists, skipping creation.")


def extract_from_mysql(**context):
    """Extract and transform data from MySQL for the exact previous hour window"""
    try:
        mysql_hook = MySqlHook(mysql_conn_id="airflow_con")

        # Get the logical date (execution_date) from the context
        execution_date = context["logical_date"]

        # Calculate the exact hour window
        window_end = execution_date
        window_start = window_end - timedelta(hours=1)

        logger.info(f"Extracting data for time window: {window_start} to {window_end}")

        # Extract records updated in the exact previous hour
        query = """
        SELECT 
            t.id as receipt_id,
            t.customer_id,
            t.last_status,
            p.post_origin as pos_origin,
            p.post_destination as pos_destination,
            t.created_at,
            t.updated_at,
            CASE 
                WHEN t.last_status = 'done' THEN t.updated_at
                ELSE NULL
            END as deleted_at
        FROM transactions t
        JOIN posts p ON t.post_id = p.post_id
        WHERE t.updated_at >= %s
        AND t.updated_at < %s
        ORDER BY t.updated_at
        """

        # Execute the query with the exact hour window
        df = mysql_hook.get_pandas_df(query, parameters=[window_start, window_end])

        if df.empty:
            logger.info(f"No records found in window: {window_start} to {window_end}")
            return None

        logger.info(
            f"Extracted {len(df)} rows from MySQL for window: {window_start} to {window_end}"
        )

        # Log some statistics about the extracted data
        status_counts = df["last_status"].value_counts()
        logger.info("Status distribution in extracted data:")
        for status, count in status_counts.items():
            logger.info(f"- {status}: {count} records")

        # Push the DataFrame to XCom
        task_instance = context["task_instance"]
        task_instance.xcom_push(key="extracted_data", value=df)
        return df

    except Exception as e:
        logger.error(f"Error in extract_from_mysql: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error in extract_from_mysql: {str(e)}")
        raise


def transform_data(**context):
    """Apply any additional transformations to the data"""
    try:
        # Get the DataFrame from the previous task
        task_instance = context["task_instance"]
        df = task_instance.xcom_pull(task_ids="extract_from_mysql")

        # Remove any duplicate receipt_ids, keeping the latest version
        df = df.sort_values("updated_at").drop_duplicates("receipt_id", keep="last")

        # Convert timestamps to datetime if they aren't already
        timestamp_columns = ["created_at", "updated_at", "deleted_at"]
        for col in timestamp_columns:
            df[col] = pd.to_datetime(df[col])
            # Replace NaT with None/NULL
            df[col] = df[col].replace({pd.NaT: None})

        # Add ETL timestamp
        df["etl_updated_at"] = datetime.now()

        # Replace any remaining NaT values with None
        df = df.replace({pd.NaT: None})

        logger.info(f"Transformed data: {len(df)} rows ready for loading")
        task_instance.xcom_push(key="transformed_data", value=df)
        return df
    except Exception as e:
        logger.error(f"Error in transform_data: {str(e)}")
        raise


def load_to_postgres(**context):
    """Load the transformed data into PostgreSQL using upsert"""
    try:
        # Get the DataFrame from the previous task
        task_instance = context["task_instance"]
        df = task_instance.xcom_pull(task_ids="transform_data")

        if df is None:
            raise ValueError("No data received from transform task")

        pg_hook = PostgresHook(postgres_conn_id="postgres_con")

        # Ensure all timestamps are either proper datetime objects or None
        timestamp_columns = ["created_at", "updated_at", "deleted_at", "etl_updated_at"]
        for col in timestamp_columns:
            if col in df.columns:
                df[col] = df[col].replace({pd.NaT: None})

        # Convert DataFrame to list of tuples
        insert_sql = """
            INSERT INTO shipping_fact (
                receipt_id, customer_id, last_status, pos_origin,
                pos_destination, created_at, updated_at, deleted_at, 
                etl_updated_at
            ) VALUES %s
            ON CONFLICT (receipt_id) 
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                last_status = EXCLUDED.last_status,
                pos_origin = EXCLUDED.pos_origin,
                pos_destination = EXCLUDED.pos_destination,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                deleted_at = EXCLUDED.deleted_at,
                etl_updated_at = EXCLUDED.etl_updated_at;
        """

        # Convert DataFrame to list of tuples
        records = df[
            [
                "receipt_id",
                "customer_id",
                "last_status",
                "pos_origin",
                "pos_destination",
                "created_at",
                "updated_at",
                "deleted_at",
                "etl_updated_at",
            ]
        ].values.tolist()

        from psycopg2.extras import execute_values

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        execute_values(cur, insert_sql, records)
        conn.commit()
        cur.close()

        logger.info(f"Successfully loaded {len(df)} rows into PostgreSQL")
    except Exception as e:
        logger.error(f"Error in load_to_postgres: {str(e)}")
        raise


def run_data_quality_checks():
    """Run data quality checks after the load"""
    pg_hook = PostgresHook(postgres_conn_id="postgres_con")
    mysql_hook = MySqlHook(mysql_conn_id="airflow_con")

    # Check 1: Compare record counts
    mysql_count = mysql_hook.get_first("SELECT COUNT(*) FROM transactions")[0]
    pg_count = pg_hook.get_first("SELECT COUNT(*) FROM shipping_fact")[0]

    if pg_count < mysql_count:
        logger.warning(
            f"Record count mismatch: MySQL={mysql_count}, PostgreSQL={pg_count}"
        )

    # Check 2: Verify no nulls in critical columns
    critical_nulls = pg_hook.get_first("""
        SELECT COUNT(*) 
        FROM shipping_fact 
        WHERE receipt_id IS NULL 
        OR customer_id IS NULL 
        OR last_status IS NULL
    """)[0]

    if critical_nulls > 0:
        logger.error(
            f"Found {critical_nulls} rows with NULL values in critical columns"
        )

    # Check 3: Verify deleted_at is set for 'done' status
    invalid_done_status = pg_hook.get_first("""
        SELECT COUNT(*) 
        FROM shipping_fact 
        WHERE last_status = 'done' 
        AND deleted_at IS NULL
    """)[0]

    if invalid_done_status > 0:
        logger.error(
            f"Found {invalid_done_status} 'done' status rows without deleted_at timestamp"
        )

    logger.info("Data quality checks completed!")


# Define DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    "mysql_to_postgres_etl",
    default_args=default_args,
    description="ETL from MySQL to PostgreSQL with status tracking",
    schedule_interval="@hourly",
    catchup=False,
)

# Define tasks
test_connections_task = PythonOperator(
    task_id="test_connections", python_callable=test_connections, dag=dag
)

create_target_table_task = PythonOperator(
    task_id="create_target_table", python_callable=create_target_table, dag=dag
)

extract_task = PythonOperator(
    task_id="extract_from_mysql",
    python_callable=extract_from_mysql,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag,
)

data_quality_task = PythonOperator(
    task_id="run_data_quality_checks",
    python_callable=run_data_quality_checks,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
(
    test_connections_task
    >> create_target_table_task
    >> extract_task
    >> transform_task
    >> load_task
    >> data_quality_task
)
