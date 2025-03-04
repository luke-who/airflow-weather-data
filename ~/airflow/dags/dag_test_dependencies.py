# Import core Airflow modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from typing import Dict, Any


def create_dag(jinja_approach: bool = True, **kwargs) -> DAG:
    """
    Factory function to create a DAG with configurable variable handling approaches.

    Parameters:
    jinja_approach (bool): True = Use Jinja templating, False = Use Python variable
    **kwargs: Arbitrary keyword arguments for DAG configuration

    Returns:
    DAG: Configured DAG object
    """

    # Default arguments for all tasks
    default_args = {
        "owner": "Ivan",  # DAG owner
        "retries": 1,  # Number of automatic retries
        "retry_delay": timedelta(minutes=5),  # Delay between retries
        "start_date": datetime(2023, 1, 1),  # Initial execution date
    }

    # Merge default DAG configuration with user-provided kwargs
    dag_kwargs = {
        "default_args": default_args,
        "schedule_interval": "*/1 * * * *",  # Run every minute
        "catchup": False,  # Disable catchup runs
        "tags": ["test"],  # Organizational tags
        **kwargs,  # Allow custom overrides
    }

    # DAG context manager
    with DAG(**dag_kwargs) as dag:
        # We can use the following bash/zsh code to test if we can write date to the weather_dir (especially if the path contains spaces):
        # mkdir -p "$(airflow variables get weather_dir)" && cd "$(airflow variables get weather_dir)" && date >> date.txt

        # Determine variable handling approach
        if jinja_approach:
            # APPROACH 1: Jinja templating
            # - Variables resolved at runtime
            # - Uses Airflow's template engine
            base_command = 'mkdir -p "{{ var.value.weather_dir }}" && cd "{{ var.value.weather_dir }}" && '
        else:
            # APPROACH 2: Python variable
            # - Variable fetched at DAG initialization
            # - Uses Python f-strings
            weather_dir = Variable.get("weather_dir")  # Get once during DAG init
            base_command = f'mkdir -p "{weather_dir}" && cd "{weather_dir}" && '

        # TASK 1: Write current date to file
        date_task = BashOperator(
            task_id="write_date",
            # Append specific command to base command
            bash_command=base_command + "date >> date.txt",
        )

        # TASK 2: Git add all changes
        add_task = BashOperator(
            task_id="add_files", bash_command=base_command + "git add ."
        )

        # TASK 3: Commit changes with message
        commit_task = BashOperator(
            task_id="commit_files",
            bash_command=base_command + 'git commit -m "Update date"',
        )

        # TASK 4: Push to remote repository
        push_task = BashOperator(
            task_id="push_files", bash_command=base_command + "git push"
        )

        # SET DEPENDENCIES: Linear workflow
        date_task >> add_task >> commit_task >> push_task

    return dag


# CONFIGURATION SECTION ========================================================

# Approach selector (True = Jinja, False = Python variable)
USE_JINJA_APPROACH = False  # Change this to switch approaches

# Base DAG configuration
dag_config = {
    "dag_id": "test_dag_dependencies",  # Unique identifier
    "description": "DAG with configurable variable handling",  # Metadata
    "start_date": datetime(2023, 1, 1),  # Override default if needed
    "max_active_runs": 1,  # Prevent concurrent runs
}

# DAG INSTANTIATION ============================================================
# Create the DAG using the factory function
dag = create_dag(
    jinja_approach=USE_JINJA_APPROACH,  # Pass approach selector
    **dag_config,  # Unpack configuration dictionary
)
