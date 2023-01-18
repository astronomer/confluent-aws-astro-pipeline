# listen_to_what_you_say.py

"""UPDATE"""

from pendulum import datetime, duration
from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
import logging
import os
import json
import uuid

from airflow_provider_kafka.operators.event_triggers_function import EventTriggersFunctionOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger('airflow.task')

# ENV 
TOPIC_NAME = "numbers"

# Dataset
sales_model = Dataset("sagemaker://train-sales-data")

def apply_function(message):
    return True
    print("hi")
    print(message)
    val = json.loads(message.value())
    print(val)
    return val
    """if val % 3 == 0:
        return val
    if val % 5 == 0:
        return val"""

def pick_downstream_dag(message, **context):
    return "hi"
    if message % 15 == 0:
        print("fizzbuzz")
        """TriggerDagRunOperator(
            trigger_dag_id="fizz_buzz", task_id=f"{message}{uuid.uuid4()}"
        ).execute(context)"""
    else:
        if message % 3 == 0:
            print("FIZZ !")
        if message & 5 == 0:
            print("BUZZ !")

@dag(
    start_date=datetime(2023,1,15),
    schedule="@hourly", # adjust schedule and dagrun_timeout based on your needs
    dagrun_timeout=duration(hours=1),
    max_active_runs=1, # a new run of this DAG is only scheduled when no active runs are present 
    catchup=False,
)
def listen_to_what_you_say():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    listen_to_confluent_messages = EventTriggersFunctionOperator(
        task_id=f"listen_to_messages_in_{TOPIC_NAME}",
        topics=[TOPIC_NAME],
        # the apply_function needs to be passed in as a module, because it needs to be discoverable by the triggerer!
        apply_function="listen_to_what_you_say.apply_function", 
        kafka_config={
            "bootstrap.servers": os.environ["BOOSTRAP_SERVER"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"],
            "group.id": "airflow_listening_dag_4"
        },
        event_triggered_function=pick_downstream_dag,
    )

    start >> listen_to_confluent_messages >> end

listen_to_what_you_say()



       