from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = { #per file
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag4',
    default_args=default_args,
    description='A simple DAG using decorators',
    schedule=timedelta(days=1), #may be cron expression as well
    start_date=datetime(2025, 11, 2),
    catchup=False, #whether to catch up missed runs
) as dag:
    task1 = BashOperator(
        task_id='RML_Mapper',
        bash_command='java -jar /opt/airflow/generated/rmlmapper.jar -m /opt/airflow/generated/mapping.rml.ttl -o /opt/airflow/generated/rdf.ttl',
    )

task1