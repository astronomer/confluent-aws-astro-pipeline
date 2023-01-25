"""This DAG is a helper to quickly create mock data in a Confluent topic
in oder to test your pipeline."""

from airflow.decorators import dag
from airflow_provider_kafka.operators.produce_to_topic import (
    ProduceToTopicOperator
)
from pendulum import datetime
from random import randint, choices
import json
import os

# import variables
from include import global_variables as gv


# Function to create 20 mock records with random variation
def producer_function():
    for i in range(20):
        yield (
            json.dumps(i),
            json.dumps(
                {
                    "Invoice": 489434+i,
                    "Description": "15CM CHRISTMAS GLASS BALL 20 LIGHTS",
                    "Customer ID": "13085",
                    "Price": 10*i,
                    "Quantity": randint(-1, 1000),
                    "Country": choices(
                        population=["United Kingdom", "US"],
                        weights=[0.9, 0.1],
                        k=1
                    )[0],
                    "InvoiceDate": f"{i}/1/2009 07:45",
                    "Distribution ID": randint(1, 10),
                    "StockCode": "85048"
                }
            )
        )


@dag(
    start_date=datetime(2023, 1, 23),
    schedule_interval=None,
    catchup=False,
    tags=["helper_dag"]
)
def helper_dag_producer():

    produce_to_topic = ProduceToTopicOperator(
        task_id="produce_to_topic",
        topic=gv.TOPIC_NAME,
        producer_function="helper_dags.helper_dag_producer.producer_function",
        kafka_config={
            "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"]
        },
    )

    produce_to_topic


helper_dag_producer()
