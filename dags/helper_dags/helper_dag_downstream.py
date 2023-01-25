"""This DAG is a helper to add as an empty downstream dependency when testing
listening patterns."""

import json
from pendulum import datetime
import os

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

@dag(
    start_date=datetime(2023, 1, 23),
    schedule=None,
    catchup=False,
    tags=["helper_dag"]
)
def helper_dag_downstream():

    t1 = EmptyOperator(task_id="t1")

helper_dag_downstream()
