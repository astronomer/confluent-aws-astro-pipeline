"UPDATE"

import json
from pendulum import datetime
import os

from airflow import DAG
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

TOPIC_NAME = "numbers"

def producer_function():
    for i in range(20):
        yield (json.dumps(i), json.dumps(i + 1))

with DAG(
    dag_id="helper_dag_producer",
    description="Examples of Kafka Operators",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["helper_dag"]
) as dag:

    t1 = ProduceToTopicOperator(
        task_id="produce_to_topic",
        topic=TOPIC_NAME,
        producer_function="helper_dag_producer.producer_function",
        kafka_config={"bootstrap.servers": os.environ["BOOSTRAP_SERVER"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"]},
    )

   
