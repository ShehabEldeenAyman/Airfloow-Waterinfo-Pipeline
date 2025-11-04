from datetime import datetime, timedelta
from airflow.decorators import dag, task

from rdflib import Graph,URIRef,Namespace,BNode,Literal
from rdflib.namespace import XSD,RDF
import pandas as pd
import argparse
from collections import defaultdict
from datetime import datetime
import json

default_args = { #per file
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

@dag( #per file
    dag_id='da5',
    default_args=default_args,
    description='A simple DAG using decorators',
    schedule=timedelta(days=1), #may be cron expression as well
    start_date=datetime(2025, 11, 2),
    catchup=False, #whether to catch up missed runs
)

def prettify():
    @task
    def LoadGraph(directory):
        graph = Graph()
        print("Started loading graph...")
        graph.parse(directory, format="turtle",publicID="https://example.org/")
        print("Graph loaded successfully.")
        return graph
    
    @task
    def SaveGraph(directory, final_graph):
        print('Started writing file to disk')
        final_graph.serialize(destination=directory, format="turtle")
        print('File written successfully')

################################################################################

    input_path = "/opt/airflow/generated/rdf.ttl"
    output_path = "/opt/airflow/generated/rdf_prettified.ttl"
    Original_graph  = LoadGraph(input_path)
    SaveGraph(output_path,Original_graph)
prettify_dag = prettify()
    