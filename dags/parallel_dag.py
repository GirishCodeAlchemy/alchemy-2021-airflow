import os
from datetime import datetime

import yaml
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from utils.logging import get_logging_client

logging = get_logging_client()

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
        if operator == 'BashOperator':
            bash_command = task_info.get('bash_command')
            task = BashOperator(
                task_id=task_id,
                bash_command=bash_command,
                dag=self.dag
            )
        else:
            task = DummyOperator(
                task_id=task_id,
                dag=self.dag
            )
        return task_id, task

    def create_child_task_group(self, task_group_info ):
        task_group_id = task_group_info['task_id']
        with TaskGroup(group_id=task_group_id, dag=self.dag) as task_group:
            previous_dag = None
            for task_info in task_group_info['task_group']:
                _, task = self.create_task(task_info)
                if previous_dag == None:
                    previous_dag = task
                else:
                    previous_dag = previous_dag >> task
                logging.info(f"previous_dag {previous_dag}")

        return task_group

    def create_task_group(self, task_group_info ):
        task_group_id = task_group_info['task_id']
        task_group_list = []

        with TaskGroup(group_id=task_group_id, dag=self.dag) as task_group:
            for task_info in task_group_info['task_group']:
                if 'task_group' in task_info:
                    task_group_list.append(self.create_child_task_group(task_info))
                else:
                    _, task = self.create_task(task_info)
                    task_group_list.append(task)
        logging.info(f"list of task group: {task_group_list}")
        return task_group

    def build(self):
        self.load_config()
        self.parse_dag_config()
        self.create_dag()

        tasks = self.dag_config['dag']['tasks']
        previous_dag = None
        for task_info in tasks:
            result_dag = None
            if 'task_group' in task_info:
                result_dag = self.create_task_group(task_info)
            else:
                task_id, result_dag = self.create_task(task_info)
                self.task_dict[task_id] = result_dag
            logging.info(f"---> previous_dag = {previous_dag}  -->  current task ={result_dag} , ")
            if previous_dag == None:
                previous_dag = result_dag
            else:
                previous_dag = previous_dag >> result_dag

        logging.info('Task dictionary={}'.format(self.task_dict))
        return self.dag

def create_dag_from_yaml(yaml_file):
    builder = DAGBuilder(yaml_file)
    return builder.build()

directory_path = os.path.dirname(__file__)
application_configuration_file_path = os.path.join(directory_path, 'parallel.yaml')
dag = create_dag_from_yaml(application_configuration_file_path)
