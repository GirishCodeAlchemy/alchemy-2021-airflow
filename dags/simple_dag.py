from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id='simple_example',
    schedule_interval='@daily',
    start_date=datetime(2024, 3, 20),
    catchup=False,
    default_args={'owner': 'airflow', 'depends_on_past': False, 'retries': 3},
) as dag:
    start = DummyOperator(task_id='start', priority_weight=10, queue='default')
    task_group_0 = BashOperator(task_id='task-group-0', bash_command="echo 'Task 0'")

    with TaskGroup('task-group1') as task_group1:
        task1_1 = BashOperator(task_id='task1_1', bash_command="echo 'Task1 1'")
        task1_2 = BashOperator(task_id='task1_2', bash_command="echo 'Task1 2'")
        task1_3 = BashOperator(task_id='task1_3', bash_command="echo 'Task1 3'")
        task1_1 >> task1_2
        task1_2 >> task1_3

    with TaskGroup('task-group2') as task_group2:
        task2_1 = BashOperator(task_id='task2_1', bash_command="echo 'Task2 1'")
        task2_2 = BashOperator(task_id='task2_2', bash_command="echo 'Task2 2'")
        task2_3 = BashOperator(task_id='task2_3', bash_command="echo 'Task2 3'")
        task2_1 >> task2_2 >> task2_3

    end = DummyOperator(task_id='end', priority_weight=1, queue='default')

    start >> [task_group_0, task_group1, task_group2] >> end
