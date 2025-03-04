# This script defines an Apache Airflow DAG that runs a Bash command to write the current date 
# to a file on the user's Desktop every minute.

# Import necessary modules from Airflow
from airflow.models import DAG  # For defining the Directed Acyclic Graph (DAG)
from datetime import datetime  # For working with dates and times
from datetime import timedelta  # For defining time intervals
from airflow.operators.bash_operator import BashOperator  # For running Bash commands

# Define default arguments for the DAG
default_args = {
    "owner": "Ivan",  # The owner of the DAG
    "depends_on_past": False,  # Whether the task depends on the previous run
    "email": ["ivan@theaicore.com"],  # Email address for notifications
    "email_on_failure": False,  # Whether to send emails on task failure
    "email_on_retry": False,  # Whether to send emails on task retry
    "retries": 1,  # Number of retries if the task fails
    "start_date": datetime(2025, 3, 1),  # Start date for the DAG
    "retry_delay": timedelta(minutes=5),  # Delay between retries
    "end_date": datetime(2050, 1, 1),  # End date for the DAG
    # 'queue': 'bash_queue',  # Optional: Queue to which the task is assigned
    # 'pool': 'backfill',  # Optional: Pool to which the task is assigned
    # 'priority_weight': 10,  # Optional: Priority weight of the task
    # 'wait_for_downstream': False,  # Optional: Whether to wait for downstream tasks
    # 'dag': dag,  # Optional: Reference to the DAG object
    # 'trigger_rule': 'all_success'  # Optional: Rule for triggering the task
}

# Define the DAG
with DAG(
    dag_id="test_dag",  # Unique identifier for the DAG
    default_args=default_args,  # Default arguments for the DAG
    schedule_interval="*/1 * * * *",  # Schedule interval (every 1 minute)
    catchup=False,  # Whether to backfill past runs
    tags=["test"],  # Tags for categorizing the DAG
) as dag:
    # Define the tasks. Here we are going to define only one BashOperator
    test_task = BashOperator(
        task_id="write_date_file",  # Unique identifier for the task
        bash_command="cd ~/Desktop && date >> ai_core.txt",  # Bash command to execute
        dag=dag,  # Reference to the DAG
    )
