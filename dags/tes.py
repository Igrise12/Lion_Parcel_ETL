from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definisikan default_args untuk DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 19),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "print_ds_next_ds",
    default_args=default_args,
    description="A DAG to print ds and next_ds",
    schedule_interval="0 8 * * *",
    catchup=False,
) as dag:
    print_date_info = BashOperator(
        task_id="print_date_info",
        bash_command="""
            echo "Date Information:"
            echo "DS (execution date)     : {{ ds }}"
            echo "Next DS (next run date) : {{ next_ds }}"
            echo "Timestamp              : {{ ts }}"
            echo "Execution Date Time    : {{ execution_date }}"
            echo "Next Execution Time    : {{ next_execution_date }}"
            echo "Hour                   : {{ execution_date.hour }}"
        """,
    )

print_date_info
