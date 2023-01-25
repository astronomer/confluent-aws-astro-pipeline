"""This DAG shows an advanced pattern including Airflow and Confluent. The 
listen_to_confluent_messages task will listen to the specified Confluent topic
and evaluate each message through the function supplied as `apply_function`.
If the `apply_function` returns a value, the `event_triggered_function` will
run. Afterwards the task goes back to listening. 
After one hour the DAG will time out and a new Dagrun will be kicked off."""


from airflow import Dataset
from airflow.decorators import dag
from airflow_provider_kafka.operators.event_triggers_function import (
    EventTriggersFunctionOperator
)
from airflow.providers.slack.operators.slack_webhook import (
    SlackWebhookOperator
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime, duration
import logging
import os
import json
import uuid

# import variables
from include import global_variables as gv

log = logging.getLogger('airflow.task')
log_processor = logging.getLogger('airflow.processor')

# Dataset
sales_model = Dataset("sagemaker://train-sales-data")


def slack_failure_function(message, context):
    """Posts a Slack message in the QA channel."""

    slack_msg = """
        An order with a negative Quantity was posted!
        *Execution Time*: {exec_date},
        *Message*: {message}
        """.format(
        exec_date=context.get('execution_date'),
        message=message
    )

    neg_quant_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id=gv.SLACK_CONN_ID,
        message=slack_msg
    )

    return neg_quant_alert.execute(context=context, message=message)


def check_messages(message):
    """Function that will run on each message in the Confluent topic. If this
    function returns any value, the `event_triggered_function` will run. The
    value returned by this function will be passed into the
    `event_triggered_function` as `message`.
    
    This function will check if the price value in the message is above 180,
    the order was placed from the US or the quantity of order is negative.
    In all of these cases it will return the full message content to be used
    in the `event_triggered_function`.
    
    This function runs within the Triggerer component. View logs with 
    `astro dev logs -t`."""

    log_processor.info(f"Message consumed: {message}")
    val = json.loads(message.value())
    log_processor.info(f"Message content: {val}")
    if val['Price'] > 180 and val["Quantity"] < 300:
        log_processor.info(
            f"Price was {val['Price']}, \
                triggering event_triggered_function."
        )
        return val
    if val['Country'] == "US":
        log_processor.info(
            f"Order was placed from the US, \
                triggering event_triggered_function."
        )
        return val
    if val['Quantity'] < 0:
        log_processor.info(
            f"Order was placed from the US, \
                triggering event_triggered_function."
        )
        return val


def pick_downstream_dag(message, **context):
    """This function runs if a message causes the `apply_function` to return
    any value.

    If the message contains an order with a price above 180 at a quantity below
    300 it will kick of a downstream DAG to run an out-of-schedule retraining 
    of the SageMaker model.
    If the message shows an order from the US the the function will kick
    off a downstream DAG which saved this message in a
    separate relational database to feed into US customer service dashboards.
    If the message contains an order with a negative quantity a message will be
    posted to the QA Slack channel to.
    """

    log.info(f"Message received: {message}")
    if message['Price'] > 180 and message["Quantity"] < 300:
        log.info(
            f"Price was {message['Price']} at a Quantity of \
                {message['Quantity']}, triggering extra model retraining."
        )
        TriggerDagRunOperator(
            trigger_dag_id="extra_model_retrain_dag",
            task_id=f"triggered_downstream_dag_{uuid.uuid4()}"
        ).execute(context)
    if message['Country'] == "US":
        log.info(
            f"An order from the US was detected. Running data collection DAG."
        )
        TriggerDagRunOperator(
            trigger_dag_id="collect_US_orders_in_db",
            task_id=f"triggered_downstream_dag_{uuid.uuid4()}",
            # provide the message to the downstream DAG
            conf={"message": message}
        ).execute(context)
    if message['Quantity'] < 0:
        if gv.QA_SLACK_ALERTS:
            slack_failure_function(message)
        else:
            log.info(f"Slack alerts for negative order quantities disabled.")

@dag(
    start_date=datetime(2023, 1, 23),
    # adjust schedule and dagrun_timeout based on your needs
    schedule="@hourly",
    # the DAG times out after 1 hour, only one Dagrun can be active at any time
    dagrun_timeout=duration(hours=1),
    max_active_runs=1,
    catchup=False,
    tags=["listen_pattern"]
)
def listen_to_what_you_say():

    listen_to_confluent_messages = EventTriggersFunctionOperator(
        task_id=f"listen_to_messages_in_{gv.TOPIC_NAME}",
        topics=[gv.TOPIC_NAME],
        # the apply_function needs to be passed in as a module, because it needs to be discoverable by the triggerer!
        apply_function="listen_to_what_you_say.check_messages", 
        kafka_config={
            "bootstrap.servers": os.environ["BOOSTRAP_SERVER"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["KAFKA_API_KEY"],
            "sasl.password": os.environ["KAFKA_API_SECRET"],
            "group.id": "airflow_listening_dag_8",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        event_triggered_function=pick_downstream_dag,
    )


listen_to_what_you_say()



       