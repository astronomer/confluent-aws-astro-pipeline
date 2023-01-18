"""UPDATE"""

from pendulum import datetime, duration
from airflow import Dataset
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.s3 import (
    S3ListOperator, S3CopyObjectOperator, S3DeleteObjectsOperator
)
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

def turn_key_list_into_pairs(key):
    print(key)

    return {
        "source_bucket_key" : key,
        "dest_bucket_key" : key
    }

@dag(
    start_date=datetime(2023,1,15),
    schedule=None,
    dagrun_timeout=duration(days=1), # adjust dagrun timeout based on your needs
    max_active_runs=1, # a new run of this DAG is only scheduled when no active runs are present 
    render_template_as_native_obj=True,
    catchup=False,
)
def extra_model_retrain_dag():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    list_keys_in_s3 = S3ListOperator(
        task_id="list_keys_in_s3",
        aws_conn_id=AWS_CONN_ID,
        bucket=S3_INGEST_BUCKET,
        prefix=f"topics/{TOPIC_NAME}"
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
        keys=list_keys_in_s3.output,
        trigger_rule="all_failed" ####### FOR TESTING, DELETE LATER
    )

    chain(
        start,
        [list_keys_in_s3, retrain_model],
        move_files_to_storage_bucket,
        delete_files_from_ingest_bucket,
        end
    )

extra_model_retrain_dag()



       