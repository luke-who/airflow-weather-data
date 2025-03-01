from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    "owner": "Ivan",
    "depends_on_past": False,
    "email": ["ivan@theaicore.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "start_date": datetime(2023, 1, 1),  # Changed to past date for immediate execution
    "retry_delay": timedelta(minutes=5),
    "end_date": datetime(2030, 1, 1),
}

with DAG(
    dag_id="test_dag_dependencies",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
    tags=["test"],
) as dag:

    # Create directory if not exists and write date
    date_task = BashOperator(
        task_id="write_date",
        bash_command='mkdir -p "{{ var.value.weather_dir }}" && '
        'cd "{{ var.value.weather_dir }}" && '
        "date >> date.txt",
    )

    # Git add command
    add_task = BashOperator(
        task_id="add_files",
        bash_command='cd "{{ var.value.weather_dir }}" && ' "git add .",
    )

    # Git commit command
    commit_task = BashOperator(
        task_id="commit_files",
        bash_command='cd "{{ var.value.weather_dir }}" && '
        'git commit -m "Update date"',
    )

    # Git push command
    push_task = BashOperator(
        task_id="push_files",
        bash_command='cd "{{ var.value.weather_dir }}" && ' "git push",
    )

    # Simplified task dependencies
    date_task >> add_task >> commit_task >> push_task
