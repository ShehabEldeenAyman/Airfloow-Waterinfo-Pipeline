from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pywaterinfo import Waterinfo
import pandas as pd

default_args = { #per file
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag( #per file
    dag_id='dag3',
    default_args=default_args,
    description='A simple DAG using decorators',
    schedule=timedelta(days=1), #may be cron expression as well
    start_date=datetime(2025, 11, 2),
    catchup=False, #whether to catch up missed runs
)

def waterinfo_etl(): #Class
    @task
    def fetch_river_stage_2D():
        vmm = Waterinfo("vmm", cache=False)
        hic = Waterinfo("hic", cache=False)
        Gent_Terneuzen_River_Stage = hic.get_timeseries_values("98536010",period='P2D')
        Gent_Terneuzen_River_Stage["Timestamp"] = Gent_Terneuzen_River_Stage["Timestamp"].dt.tz_localize(None)
        Gent_Terneuzen_River_Stage["Timestamp"] = Gent_Terneuzen_River_Stage["Timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        return Gent_Terneuzen_River_Stage

    @task
    def fetch_river_stage_30M():
        hic = Waterinfo("hic", cache=False)
        Gent_Terneuzen_River_Stage_30_min = hic.get_timeseries_values("98536010",period='PT30M')
        Gent_Terneuzen_River_Stage_30_min["Timestamp"] = Gent_Terneuzen_River_Stage_30_min["Timestamp"].dt.tz_localize(None)
        Gent_Terneuzen_River_Stage_30_min["Timestamp"] = Gent_Terneuzen_River_Stage_30_min["Timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        return Gent_Terneuzen_River_Stage_30_min

    @task
    def concat(Gent_Terneuzen_River_Stage,Gent_Terneuzen_River_Stage_30_min):
        Gent_Terneuzen_River_Stage_combined = pd.concat([Gent_Terneuzen_River_Stage,Gent_Terneuzen_River_Stage_30_min]).drop_duplicates(subset=["Timestamp"]).reset_index(drop=True)
        return Gent_Terneuzen_River_Stage_combined

    @task
    def debug_save(Gent_Terneuzen_River_Stage_combined):        
        output_path = "/opt/airflow/generated/Gent_Terneuzen_River_Stage_combined.csv"
        Gent_Terneuzen_River_Stage_combined.to_csv(output_path)
    ################################################################################
    Gent_Terneuzen_River_Stage = fetch_river_stage_2D()
    Gent_Terneuzen_River_Stage_30_min = fetch_river_stage_30M()
    Gent_Terneuzen_River_Stage_combined = concat(Gent_Terneuzen_River_Stage,Gent_Terneuzen_River_Stage_30_min)
    debug_save(Gent_Terneuzen_River_Stage_combined)
waterinfo_dag = waterinfo_etl()
