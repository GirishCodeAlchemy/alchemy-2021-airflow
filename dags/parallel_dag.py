import os
from datetime import datetime

import yaml
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup


class DAGBuilder:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.dag_config = None
        self.dag_id = None
        self.schedule_interval = None
        self.start_date = None
        self.dag = None
        self.task_dict = {}

    def load_config(self):
        with open(self.yaml_file, 'r') as file:
            self.dag_config = yaml.safe_load(file)

    def parse_dag_config(self):
        self.dag_id = self.dag_config['dag']['dag_id']
        self.schedule_interval = self.dag_config['dag']['schedule_interval']
        start_date_str = str(self.dag_config['dag']['start_date'])
        self.start_date = datetime.strptime(start_date_str, '%Y-%m-%d')

    def create_dag(self):
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': self.start_date,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 3,
        }

        self.dag = DAG(
            dag_id=self.dag_id,
            schedule_interval=self.schedule_interval,
            default_args=default_args,
            catchup=False
        )

    def create_task(self, task_info):
        task_id = task_info['task_id']
        operator = task_info['operator']
        if operator == 'BashOperator':  # Handle BashOperator tasks
            bash_command = task_info.get('bash_command')  # Retrieve bash_command if available
            task = BashOperator(
                task_id=task_id,
                bash_command=bash_command,
                dag=self.dag
            )
        else:  # Handle DummyOperator tasks
            task = DummyOperator(
                task_id=task_id,
                dag=self.dag
            )
        self.task_dict[task_id] = task
        if task_id != 'start':  # Skip setting dependency for the start task
            task >> self.task_dict['start']  # Set dependency flow from start task

    def create_task_group(self, task_group_info):
        task_group_id = task_group_info['task_id']
        with TaskGroup(group_id=task_group_id, dag=self.dag) as task_group:
            for task_info in task_group_info['task_group']:
                self.create_task(task_info)
        return task_group

    def build(self):
        self.load_config()
        self.parse_dag_config()
        self.create_dag()

        tasks = self.dag_config['dag']['tasks']
        for task_info in tasks:
            if 'task_group' in task_info:
                self.create_task_group(task_info)
            else:
                self.create_task(task_info)

        return self.dag

# Example usage:
def create_dag_from_yaml(yaml_file):
    builder = DAGBuilder(yaml_file)
    return builder.build()

directory_path = os.path.dirname(__file__)
application_configuration_file_path = os.path.join(directory_path, 'parallel.yaml')
dag = create_dag_from_yaml(application_configuration_file_path)
