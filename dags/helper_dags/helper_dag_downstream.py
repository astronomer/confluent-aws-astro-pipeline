"UPDATE"

import json
from pendulum import datetime
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="helper_dag_downstream",
    description="Examples of Kafka Operators",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["helper_dag"]
) as dag:

    t1 = EmptyOperator(task_id="t1")
