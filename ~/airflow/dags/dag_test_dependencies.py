from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

weather_dir = Variable.get("weather_dir")
# weather_dir = "~/Library/CloudStorage/GoogleDrive-luke.z0423@gmail.com/My Drive/Colab Notebooks/AiCore/Courses/AWS Data Engineering Services/airflow-weather-data"

default_args = {
    "owner": "Ivan",
    "depends_on_past": False,
    "email": ["ivan@theaicore.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "start_date": datetime(
        2025, 3, 1
    ),  # If you set a datetime previous to the curernt date, it will try to backfill
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
    # Define the tasks. Here we are going to define only one bash operator
    date_task = BashOperator(
        task_id="write_date",
        bash_command="cd weather_dir && date >> date.txt",
        dag=dag,
    )
    add_task = BashOperator(
        task_id="add_files",
        bash_command="cd weather_dir && git add .",
        dag=dag,
    )
    commit_task = BashOperator(
        task_id="commit_files",
        bash_command='cd weather_dir && git commit -m "Update date"',
        dag=dag,
    )
    push_task = BashOperator(
        task_id="push_files",
        bash_command="cd weather_dir && git push",
        dag=dag,
    )

    date_task >> add_task >> commit_task
    add_task >> push_task
    commit_task >> push_task
