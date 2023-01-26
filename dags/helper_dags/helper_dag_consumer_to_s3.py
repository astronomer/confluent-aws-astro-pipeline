"""This DAG is a helper to consume data from your Confluent topic into
a S3 bucket."""

from airflow.decorators import dag, task
from airflow_provider_kafka.operators.consume_from_topic import (
    ConsumeFromTopicOperator
)
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)
from pendulum import datetime
import logging
import json
import os
import uuid

# import variables
from include import global_variables as gv

# get Airflow task logger
log = logging.getLogger('airflow.task')


def consumer_func(message):
    """Function to log each message and write it to a temporary JSON file."""

    key = json.loads(message.key())
    value = json.loads(message.value())
    log.info(
        f"{message.topic()} @ {message.offset()}; {key} : {value}"
    )
    with open("consumed_tmp.json", 'a+') as f:
        json.dump(value, f)
        f.write("\n")
    return value


@dag(
    start_date=datetime(2023, 1, 23),
    schedule_interval=None,
    catchup=False,
    tags=["helper_dag"]
)
def helper_dag_consumer_to_s3():

    consume_from_topic = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=[gv.TOPIC_NAME],
        apply_function="helper_dags.helper_dag_consumer_to_s3.consumer_func",
        consumer_config={
            "bootstrap.servers": os.environ["BOOTSTRAP_SERVER"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"],
            "group.id": "airflow_to_s3_consumer",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=100,
        max_batch_size=5,
    )

    load_to_s3 = LocalFilesystemToS3Operator(
        task_id="load_to_s3",
        dest_bucket=gv.S3_INGEST_BUCKET,
        dest_key="topics/{t}/year={y}/month={m}/day={d}/hour={h}/{u}.{f}".format(
            t=gv.TOPIC_NAME,
            y="{{ dag_run.logical_date.year }}",
            m="{{ dag_run.logical_date.month }}",
            d="{{ dag_run.logical_date.day }}",
            h="{{ dag_run.logical_date.hour }}",
            u=uuid.uuid4(),
            f=gv.FILE_TYPE
        ),
        filename="consumed_tmp.json",
        aws_conn_id="aws_conn"
    )

    @task
    def delete_tmp_file():
        """This task deletes the temporary local JSON file."""
        os.remove("consumed_tmp.json")

    consume_from_topic >> load_to_s3 >> delete_tmp_file()


helper_dag_consumer_to_s3()
