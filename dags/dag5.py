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
    def load_and_prettify(input_path: str, output_path: str) -> str:
        """Load an RDF file and immediately write the prettified version to disk."""
        print("Started loading RDF graph...")
        g = Graph()
        g.parse(input_path, format="turtle", publicID="https://example.org/")
        print("Graph loaded successfully. Serializing...")
        g.serialize(destination=output_path, format="turtle")
        print(f"Prettified file written to: {output_path}")
        return output_path  # return only the file path (string), not the Graph object

    input_path = "/opt/airflow/generated/rdf.ttl"
    output_path = "/opt/airflow/generated/rdf_prettified.ttl"

    load_and_prettify(input_path, output_path)

prettify_dag = prettify()
    