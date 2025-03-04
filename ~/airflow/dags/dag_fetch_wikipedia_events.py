from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from wikipedia_events_fetcher import WikipediaEventsFetcher

# Define default arguments for the DAG
default_args = {
    "owner": "USER",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(
        2024, 1, 1
    ),  # Make sure this date is in the past for the DAG to run immediately
}

# Define the Airflow DAG
with DAG(
    dag_id="wikipedia_on_this_day_dag",
    default_args=default_args,
    description="A DAG to fetch historical events for the current day from Wikipedia",
    schedule_interval="@daily",  # The DAG will run daily
) as dag:

    def fetch_wikipedia_events():
        fetcher = WikipediaEventsFetcher()  # Initiate the class and use its methods
        fetcher.get_today_events()

    # Define the PythonOperator task to call the encapsulated class
    fetch_events_task = PythonOperator(
        task_id="fetch_wikipedia_events_task",
        python_callable=fetch_wikipedia_events,
    )

# Place this code in the Airflow DAG folder to run it (usually ~/airflow/dags/).
