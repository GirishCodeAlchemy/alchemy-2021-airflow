import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

import datetime
import os
import yaml

from utils import utils
from utils.logging import get_logging_client


logging = get_logging_client()


def create_dag(dag_id, schedule, default_args, command, description):
    with DAG(
            dag_id=dag_id,
            schedule_interval=schedule,
            default_args=default_args,
            description=description,
            dagrun_timeout=datetime.timedelta(minutes=60)
    ) as dag:
        bash_operator = BashOperator(
            task_id=dag_id,
            bash_command=command,
            dag=dag
        )
    return dag


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

directory_path = os.path.dirname(__file__)
application_configuration_file_path = os.path.join(directory_path, 'conversion.yaml')

logging.info('Airflow directory path of zip={}'.format(directory_path))

application_configuration_file = utils.get_file_from_zip(application_configuration_file_path)
application_configuration = yaml.safe_load(application_configuration_file)

for scheduler in application_configuration['airflow']['schedulers']:
    dag_id = scheduler['name']
    globals()[dag_id] = create_dag(
        scheduler['name'],
        scheduler['schedule'],
        default_args,
        scheduler['command'],
        scheduler['description']
    )
