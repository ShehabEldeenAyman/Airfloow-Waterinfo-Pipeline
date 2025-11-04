from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def greet(ti):
    first_name = ti.xcom_pull(task_ids='return_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='return_name', key='last_name')
    age = ti.xcom_pull(task_ids='return_age', key='age')
    print(f"Hello, {first_name} {last_name}! You are {age} years old.")

def return_name(ti):
    #return "Shahoob"
    ti.xcom_push(key='first_name', value='Shahshoob')
    ti.xcom_push(key='last_name', value='elhawa')

def return_age(ti):
    ti.xcom_push(key='age', value=25)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag1',
    default_args=default_args,
    description='A simple DAG',
    schedule=timedelta(days=1),
    start_date=datetime(2025, 10, 30),
    catchup=False,
) as dag:
    
    greet_task = PythonOperator(
        task_id='greet',
        python_callable=greet,
        #op_kwargs={'age': 30},
    )
    name_task = PythonOperator(
        task_id='return_name',
        python_callable=return_name,
    )
    age_task = PythonOperator(
        task_id='return_age',
        python_callable=return_age,
    )

    [name_task,age_task] >> greet_task

# greet_task