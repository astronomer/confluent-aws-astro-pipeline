"""This DAG shows a simple pipeline waiting on files from Confluent to drop in
an S3 sink and deciding whether or not to retrain the Sagemaker model based on
the number of files which landed.

After the retraining of the SageMaker model has completed the DAG kicks of a
downstream DAG using a dataset."""

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
from airflow.operators.python import BranchPythonOperator
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


def model_training_decision_function(keys_in_S3):
    """Function to decide if the SageMaker model will be retrained based on
    the number of FILE_TYPE files in the S3 bucket."""

    log.info(f"{gv.TOPIC_NAME} contains the following keys: {keys_in_S3}")

    relevant_files_in_s3 = [
        f for f in keys_in_S3[0] if f.split(".")[-1] == f"{gv.FILE_TYPE}"
    ]
    num_files = len(relevant_files_in_s3)

    log.info(f"{gv.TOPIC_NAME} contains: {relevant_files_in_s3}")
    log.info(
        f"""Number of files: {num_files};
        {gv.NUM_FILES_FOR_RETRAIN} required for retraining."""
    )

    if num_files >= gv.NUM_FILES_FOR_RETRAIN:
        return "retrain_model"
    else:
        return "end"


def turn_key_list_into_pairs(key):
    """Helper function to convert a list of S3 keys into a list of
    source and dest key pairs dictionaries for dynamic task mapping."""

    return {
        "source_bucket_key": key,
        "dest_bucket_key": key
    }


@dag(
    start_date=datetime(2023, 1, 23),
    # adjust schedule and dagrun_timeout based on your needs
    schedule="@daily",
    dagrun_timeout=duration(days=1),
    # a new run of this DAG is only scheduled when no active runs are present
    max_active_runs=1,
    render_template_as_native_obj=True,
    catchup=False,
    tags=["meet_pattern"]
)
def meet_at_s3():

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

    # Depending on number of files in S3_INGEST_BUCKET, kick of retrain_model
    model_training_decision = BranchPythonOperator(
        task_id="model_training_decision",
        python_callable=model_training_decision_function,
        op_kwargs={
            "keys_in_S3": "{{ ti.xcom_pull(task_ids=['list_keys_in_s3']) }}"
        }
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
        model_training_decision
    )

    model_training_decision >> [retrain_model, end]

    chain(
        retrain_model,
        move_files_to_storage_bucket,
        delete_files_from_ingest_bucket,
        end
    )


meet_at_s3()
