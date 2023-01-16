"""This DAG shows a simple pipeline waiting on files from Confluent to drop in
an S3 sink and deciding whether or not to retrain the Sagemaker model based on 
the number of files.

After the retraining of the SageMaker model has completed the DAG kicks of a down
stream DAG using a dataset."""

from pendulum import datetime, duration
from airflow import Dataset
from airflow.decorators import dag, task 
from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensorAsync
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator, S3CopyObjectOperator, S3DeleteObjectsOperator
)
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain
import logging

log = logging.getLogger('airflow.task')

# ENV 
S3_INGEST_BUCKET = "example-ingest-bucket"
S3_STORAGE_BUCKET = "confluent-storage-bucket"
TOPIC_NAME = "ingest"
FILE_TYPE = ".json"
S3_KEY_PATH = f"topics/{TOPIC_NAME}/year=*/month=*/day=*/hour=*/*{FILE_TYPE}"
AWS_CONN_ID = "aws_conn"
NUM_FILES_FOR_RETRAIN = 1

# Dataset
sales_model = Dataset("sagemaker://train-sales-data")

def model_training_decision_function(keys_in_S3):
    """Function to decide if the SageMaker model will be retrained based on
    the number of files in the S3 bucket."""

    log.info(f"{TOPIC_NAME} contains the following keys: {keys_in_S3}")

    relevant_files_in_s3 = [
        f for f in keys_in_S3[0] if f[-5:] == f"{FILE_TYPE}"
    ]
    num_files = len(relevant_files_in_s3)
    
    log.info(f"{TOPIC_NAME} contains the following files: {relevant_files_in_s3}")
    log.info(f"Number of files: {num_files}; {NUM_FILES_FOR_RETRAIN} required for retraining.")

    if num_files >= NUM_FILES_FOR_RETRAIN:
        return "start_retraining"
    else:
        return "end"


def turn_key_list_into_pairs(key):
    print(key)

    return {
        "source_bucket_key" : key,
        "dest_bucket_key" : key
    }

@dag(
    start_date=datetime(2023,1,15),
    schedule="@daily", # adjust schedule and dagrun_timeout based on your needs
    dagrun_timeout=duration(days=1),
    max_active_runs=1, # a new run of this DAG is only scheduled when no active runs are present 
    render_template_as_native_obj=True,
    catchup=False,
)
def meet_at_s3():

    start_retraining = EmptyOperator(task_id="start_retraining")
    end = EmptyOperator(task_id="end")

    check_for_new_files_s3 = S3KeySensorAsync(
        task_id="check_for_new_files_s3",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=S3_INGEST_BUCKET,
        bucket_key=S3_KEY_PATH,
        wildcard_match=True
    )

    list_keys_in_s3 = S3ListOperator(
        task_id="list_keys_in_s3",
        aws_conn_id=AWS_CONN_ID,
        bucket=S3_INGEST_BUCKET,
        prefix=f"topics/{TOPIC_NAME}"
    )

    model_training_decision = BranchPythonOperator(
        task_id="model_training_decision",
        python_callable=model_training_decision_function,
        op_kwargs={"keys_in_S3" : "{{ ti.xcom_pull(task_ids=['list_keys_in_s3']) }}"}
    )

    retrain_model = EmptyOperator(
        task_id="TO_IMPLEMENT_retrain_model",
        # WAIT FOR COMPLETION
        outlets=[sales_model]
    )

    key_pairs = list_keys_in_s3.output.map(turn_key_list_into_pairs)

    move_files_to_storage_bucket = S3CopyObjectOperator.partial(
        task_id="move_files_to_storage_bucket",
        aws_conn_id=AWS_CONN_ID,
        source_bucket_name=S3_INGEST_BUCKET,
        dest_bucket_name=S3_STORAGE_BUCKET
    ).expand_kwargs(key_pairs)

    delete_files_from_ingest_bucket = S3DeleteObjectsOperator(
        task_id="delete_files_from_ingest_bucket",
        aws_conn_id=AWS_CONN_ID,
        bucket=S3_INGEST_BUCKET,
        keys=list_keys_in_s3.output
    )

    check_for_new_files_s3 >> list_keys_in_s3 >> model_training_decision
    
    model_training_decision >> [start_retraining, end]

    chain(
        start_retraining,
        retrain_model,
        move_files_to_storage_bucket,
        delete_files_from_ingest_bucket,
        end
    )

meet_at_s3()



       