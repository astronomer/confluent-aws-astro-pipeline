"""DAG that is scheduled to run after whenever the  SageMaker model has been 
re-trained."""

from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, duration

# Dataset
sales_model = Dataset("sagemaker://train-sales-data")

@dag(
    start_date=datetime(2023,1,15),
    # scheduled on the Dataset the SageMaker train tasks produce to
    schedule=[sales_model],
    dagrun_timeout=duration(hours=1),
    max_active_runs=1,
    catchup=False,
)
def run_after_model_training_dag():

    t1 = EmptyOperator(task_id="t1")