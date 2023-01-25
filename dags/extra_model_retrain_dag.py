"""This DAG will (re)train a SageMaker model. It does not have a schedule and
is kicked off by a TriggerDagRunOperator in the `listen_to_what_you_say` DAG.
"""

from airflow import Dataset
from airflow.decorators import dag
from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensorAsync
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator,
    S3CopyObjectOperator,
    S3DeleteObjectsOperator
)
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerTrainingOperator
)
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain
import logging
from pendulum import datetime, duration

# import variables
from include import global_variables as gv

# get Airflow task logger
log = logging.getLogger('airflow.task')

# Dataset
sales_model = Dataset("sagemaker://train-sales-data")


def turn_key_list_into_pairs(key):
    """Helper function to convert a list of S3 keys into a list of
    source and dest key pairs dictionaries for dynamic task mapping."""

    return {
        "source_bucket_key": key,
        "dest_bucket_key": key
    }


@dag(
    start_date=datetime(2023, 1, 15),
    # No schedule, DAG is kicked off by upstream TriggerDagRunOperator
    schedule=None,
    # the DAG times out after 1 day, only one Dagrun can be active at any time
    dagrun_timeout=duration(days=1),
    max_active_runs=1,
    render_template_as_native_obj=True,
    catchup=False,
    params={
        "message": None
    },
    tags=["listen_pattern"]
)
def extra_model_retrain_dag():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Async sensor checking for files to be present in S3_INGEST_BUCKET
    check_for_new_files_s3 = S3KeySensorAsync(
        task_id="check_for_new_files_s3",
        aws_conn_id=gv.AWS_CONN_ID,
        bucket_name=gv.S3_INGEST_BUCKET,
        bucket_key=gv.S3_KEY_PATH,
        wildcard_match=True
    )

    # Fetch list of all Keys in S3_INGEST_BUCKET
    list_keys_in_s3 = S3ListOperator(
        task_id="list_keys_in_s3",
        aws_conn_id=gv.AWS_CONN_ID,
        bucket=gv.S3_INGEST_BUCKET,
        prefix=f"topics/{gv.TOPIC_NAME}"
    )

    if gv.SAGEMAKER_INTERACTION:

        # Train a SageMaker model, wait until training has completed
        retrain_model = SageMakerTrainingOperator(
            task_id="retrain_model",
            config=gv.SAGEMAKER_MODEL_TRAIN_CONFIG,
            aws_conn_id=gv.AWS_CONN_ID,
            wait_for_completion=True,
            print_log=True,
            check_interval=60*5,  # sec between checks if training has finished
            max_ingestion_time=gv.MODEL_TRAINING_TIMEOUT,
            outlets=[sales_model]  # produce to the sales_model Dataset
        )

    else:

        # Empty task for pipeline testing without model interaction
        retrain_model = EmptyOperator(
            task_id="retrain_model",
            outlets=[sales_model]  # produce to the sales_model Dataset
        )

    # Create key pairs from key list (helper function, not a task)
    key_pairs = list_keys_in_s3.output.map(turn_key_list_into_pairs)

    # Mode files from S3_INGEST_BUCKET to S3_STORAGE_BUCKET
    move_files_to_storage_bucket = S3CopyObjectOperator.partial(
        task_id="move_files_to_storage_bucket",
        aws_conn_id=gv.AWS_CONN_ID,
        source_bucket_name=gv.S3_INGEST_BUCKET,
        dest_bucket_name=gv.S3_STORAGE_BUCKET
    ).expand_kwargs(key_pairs)  # dynamically mapped one task instance per file
    # if > 1024 files are expected to be moved, adjust max_map_length in the
    # Airflow config

    # Delete files from S3_INGEST_BUCKET that have been moved
    delete_files_from_ingest_bucket = S3DeleteObjectsOperator(
        task_id="delete_files_from_ingest_bucket",
        aws_conn_id=gv.AWS_CONN_ID,
        bucket=gv.S3_INGEST_BUCKET,
        keys=list_keys_in_s3.output
    )

    # Set dependencies
    chain(
        start,
        check_for_new_files_s3,
        list_keys_in_s3,
        retrain_model,
        move_files_to_storage_bucket,
        delete_files_from_ingest_bucket,
        end
    )


extra_model_retrain_dag()
