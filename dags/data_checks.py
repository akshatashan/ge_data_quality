"""
A DAG that demonstrates implementation of the GreatExpectationsOperator.
Note: you wil need to reference the necessary data assets and expectations suites in your project. You can find samples available in the provider source directory.
To view steps on running this DAG, check out the Provider Readme: https://github.com/great-expectations/airflow-provider-great-expectations#examples
"""
import os
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from operators.slack_alert import SlackAlert
from _scripts.great_expectations_checkpoints import airflow_process_check

default_args = {
    'start_date': datetime(2021, 8, 24),
    'retries': 10,
    'retry_delay': timedelta(0, 600),
    'priority_weight': 9
}

dag = DAG(
    'data-checks',
    default_args=default_args,
    description='adding quality checks for rum',
    schedule_interval='@daily'
)

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ge_root_dir = os.path.join(base_path, 'dags', 'great_expectations')


run_checkpoint = PythonOperator(
    task_id='run_checkpoint',
    priority_weight=50,
    python_callable=airflow_process_check,
    provide_context=True,
    templates_dict={'ge_root_dir': ge_root_dir, 'date': '{{ ds }}',
                    'table': 'my_table',
                    'dataset': 'my_dataset',
                    'expectation_suite_name': 'expectation_suite'},
    on_failure_callback=SlackAlert().alert,
    dag=dag)


run_checkpoint

