
### Confluent variables
TOPIC_NAME = "ingest"  # Confluent topic name
FILE_TYPE = "json"  # filetype used

### AWS variables
# name of the AWS conn ID with access to the S3 bucket and SageMaker
AWS_CONN_ID = "aws_conn"

# S3 variables
S3_INGEST_BUCKET = "example-ingest-bucket"  # Confluent S3 sink
S3_STORAGE_BUCKET = "confluent-storage-bucket"  # permanent record storage
# path to all newly ingested files in the S3 sink
S3_KEY_PATH = f"topics/{TOPIC_NAME}/year=*/month=*/day=*/hour=*/*{FILE_TYPE}"

# SageMaker / model variables
# Toggle SageMaker interaction
# if False, an EmptyOperator will run instead of the SageMakerTrainingOperator
SAGEMAKER_INTERACTION = False
# SageMaker.Client.create_training_job() configuration
SAGEMAKER_MODEL_TRAIN_CONFIG = ""
# seconds to wait for the model to finish training before failing the task
MODEL_TRAINING_TIMEOUT = 6*60*60*2
# number of files that need to be in the S3 sink to trigger model retraining
NUM_FILES_FOR_RETRAIN = 1

# Slack variables
QA_SLACK_ALERTS = True  # toggle Slack alerting
SLACK_CONN_ID = "slack_conn_id"

# Relational storage connection
POSTGRES_CONN = "postgres_conn_id"
