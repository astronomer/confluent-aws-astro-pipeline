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

# get Airflow task logger
log = logging.getLogger('airflow.task')

# ENV
S3_INGEST_BUCKET = "example-ingest-bucket"  # Confluent S3 sink
S3_STORAGE_BUCKET = "confluent-storage-bucket"  # permanent record storage
TOPIC_NAME = "ingest"  # Confluent topic name
FILE_TYPE = "json"  # filetype used

# path to all newly ingested files in the S3 sink
S3_KEY_PATH = f"topics/{TOPIC_NAME}/year=*/month=*/day=*/hour=*/*{FILE_TYPE}"
# name of the AWS conn ID with access to the S3 bucket and SageMaker
AWS_CONN_ID = "aws_conn"

# Toggle SageMaker interaction
# if False, an EmptyOperator will run instead of the SageMakerTrainingOperator
SAGEMAKER_INTERACTION = False
# SageMaker.Client.create_training_job() configuration
SAGEMAKER_MODEL_TRAIN_CONFIG = ""
# seconds to wait for the model to finish training before failing the task
MODEL_TRAINING_TIMEOUT = 6*60*60*2

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
    # No schedule, DAG is kicked off by upstream TriggerDagrunOperator
    schedule=None,
    dagrun_timeout=duration(days=1),
    # a new run of this DAG is only scheduled when no active runs are present
    max_active_runs=1,
    render_template_as_native_obj=True,
    catchup=False,
)
def extra_model_retrain_dag():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Async sensor checking for files to be present in S3_INGEST_BUCKET
    check_for_new_files_s3 = S3KeySensorAsync(
        task_id="check_for_new_files_s3",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=S3_INGEST_BUCKET,
        bucket_key=S3_KEY_PATH,
        wildcard_match=True
    )

    # Fetch list of all Keys in S3_INGEST_BUCKET
    list_keys_in_s3 = S3ListOperator(
        task_id="list_keys_in_s3",
        aws_conn_id=AWS_CONN_ID,
        bucket=S3_INGEST_BUCKET,
        prefix=f"topics/{TOPIC_NAME}"
    )

    if SAGEMAKER_INTERACTION:

        # Train a SageMaker model, wait until training has completed
        retrain_model = SageMakerTrainingOperator(
            task_id="retrain_model",
            config=SAGEMAKER_MODEL_TRAIN_CONFIG,
            aws_conn_id=AWS_CONN_ID,
            wait_for_completion=True,
            print_log=True,
            check_interval=60*5,  # s between checks if training has finished
            max_ingestion_time=MODEL_TRAINING_TIMEOUT,
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
        aws_conn_id=AWS_CONN_ID,
        source_bucket_name=S3_INGEST_BUCKET,
        dest_bucket_name=S3_STORAGE_BUCKET
    ).expand_kwargs(key_pairs)  # dynamically mapped one task instance per file
    # if > 1024 files are expected to be moved, adjust max_map_length in the
    # Airflow config

    # Delete files from S3_INGEST_BUCKET that have been moved
    delete_files_from_ingest_bucket = S3DeleteObjectsOperator(
        task_id="delete_files_from_ingest_bucket",
        aws_conn_id=AWS_CONN_ID,
        bucket=S3_INGEST_BUCKET,
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
