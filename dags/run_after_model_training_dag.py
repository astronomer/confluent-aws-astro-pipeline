"""DAG that is scheduled to run after SageMaker model has been
re-trained, both in case of regulary scheduled training and extra training."""

from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration
from airflow.providers.slack.operators.slack_webhook import (
    SlackWebhookOperator
)

# import variables
from include import global_variables as gv

# Dataset
sales_model = Dataset("sagemaker://train-sales-data")


@dag(
    start_date=datetime(2023, 1, 15),
    # scheduled on the Dataset the SageMaker train tasks produce to
    schedule=[sales_model],
    dagrun_timeout=duration(hours=1),
    max_active_runs=1,
    catchup=False,
)
def run_after_model_training_dag():

    if gv.QA_SLACK_ALERTS:

        training_finished_alert = SlackWebhookOperator(
            task_id='training_finished_alert',
            http_conn_id=gv.SLACK_CONN_ID,
            message={
                "SageMaker training finished successfully. Downstream actions \
                    running."
            }
        )

    else:

        training_finished_alert = EmptyOperator(
            task_id="training_finished_alert"
        )

    training_finished_alert

    # downstream pipeline

run_after_model_training_dag()
