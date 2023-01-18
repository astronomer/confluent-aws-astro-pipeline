from pendulum import datetime, duration
from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

# Dataset
sales_model = Dataset("sagemaker://train-sales-data")

@dag(
    start_date=datetime(2023,1,15),
    schedule=[sales_model], #runs whenever the model training dataset is updated
    dagrun_timeout=duration(hours=1),
    max_active_runs=1,
    catchup=False,
)
def run_after_model_training_dag():

    t1 = EmptyOperator(task_id="t1")