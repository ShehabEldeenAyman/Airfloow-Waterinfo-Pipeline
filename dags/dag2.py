from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = { #per file
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag( #per file
    dag_id='dag2',
    default_args=default_args,
    description='A simple DAG using decorators',
    schedule=timedelta(days=1), #may be cron expression as well
    start_date=datetime(2025, 10, 30),
    catchup=False, #whether to catch up missed runs
)

def etl_test(): #Class
    @task(multiple_outputs=True) #Function
    def get_name():
        return {
            'first_name': 'Shahshoob',
            'last_name': 'elhawa'
        }
    
    @task
    def get_age():
        return 25
    
    @task
    def greet(first_name, last_name, age):
        output_path = "/opt/airflow/generated/demofile.txt" # Use a mapped path
        print(f"Hello, {first_name} {last_name}! You are {age} years old.") 
        
        with open(output_path, "a") as f: 
            f.write(f"[{datetime.now().isoformat()}] Hello, {first_name} {last_name}! You are {age} years old.\n")
################################################################################
    name_dic = get_name()
    age = get_age()

    greet(first_name=name_dic['first_name'], last_name=name_dic['last_name'], age=age)

test_dag = etl_test()    