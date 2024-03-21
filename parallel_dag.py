import os
from datetime import datetime

import yaml
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from utils.logging import get_logging_client

logging = get_logging_client()

def create_dag_from_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        dag_config = yaml.safe_load(file)

    logging.info('DAG configuration={}'.format(dag_config))
    dag_id = dag_config['dag']['dag_id']
    schedule_interval = dag_config['dag']['schedule_interval']
    start_date_str = str(dag_config['dag']['start_date'])
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': start_date,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
    }

    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        default_args=default_args,
        catchup=False
    )

    tasks = dag_config['dag']['tasks']
    task_dict = {}

    for task_info in tasks:
        task_id = task_info['task_id']

        if 'task_group' in task_info:  # Handle parallel task group
            parallel_tasks = task_info['task_group']
            logging.info('Parallel tasks={}'.format(parallel_tasks))
            with TaskGroup(group_id=task_id, dag=dag) as task_group:  # Use group_id instead of task_id
                for parallel_task in parallel_tasks:
                    parallel_task_id = parallel_task['task_id']  # Assign a unique task_id within the TaskGroup
                    operator = parallel_task['operator']
                    if operator == 'BashOperator':  # Handle BashOperator tasks
                        bash_command = parallel_task.get('bash_command')  # Retrieve bash_command if available
                        task = BashOperator(
                            task_id=parallel_task_id,
                            bash_command=bash_command,
                            dag=dag
                        )
                    else:  # Handle DummyOperator tasks
                        task = DummyOperator(
                            task_id=parallel_task_id,
                            dag=dag
                        )
                    task_dict[parallel_task_id] = task
        else:  # Handle individual tasks
            operator = task_info['operator']
            if operator == 'BashOperator':  # Handle BashOperator tasks
                bash_command = task_info.get('bash_command')  # Retrieve bash_command if available
                task = BashOperator(
                    task_id=task_id,
                    bash_command=bash_command,
                    dag=dag
                )
            else:  # Handle DummyOperator tasks
                task = DummyOperator(
                    task_id=task_id,
                    dag=dag
                )
            task_dict[task_id] = task

    logging.info('Task dictionary={}'.format(task_dict))



    return dag

# Example usage:
directory_path = os.path.dirname(__file__)
application_configuration_file_path = os.path.join(directory_path, 'parallel.yaml')
logging.info('Airflow directory path={}'.format(directory_path))
print(application_configuration_file_path)
dag = create_dag_from_yaml(application_configuration_file_path)
