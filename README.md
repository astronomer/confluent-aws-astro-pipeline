Overview
========

Welcome to the Confluent-AWS-Astro pipeline: A repo demonstrating common patterns around using Kafka and Airflow together!

> Note of Caution: While it is possible to manage a Kafka cluster with Airflow, be aware that Airflow itself should not be used for streaming or low-latency processes. See the [Best practices section in the Kafka and Airflow tutorial](https://docs.astronomer.io/learn/airflow-kafka#best-practices) for more information.

How to use this repository
==========

1. Clone this repository.
2. Install the free and open source [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) - the quickest way to run Airflow locally.
3. Run `astro dev init` in the cloned repository to turn it into a fully functional Astro project.
4. Create a `.env` file and provide the following Kafka parameters in it as environment variables (you can use any Kafka cluster, OS, Confluent or others). 
    -  `BOOTSTRAP_SERVER`
    -  `KAFKA_API_KEY`
    -  `KAFKA_API_SECRET`
5. Run `astro dev start` to start up Airflow.
6. In your project directory navigate to `include/global_variables.py`. Configure the project to your liking.
7. Provide the connection to AWS using an [Airflow connection](https://docs.astronomer.io/learn/connections) named `aws_conn`.
8. If you want to use the Slack and Postgres functionality provide a Slackwebhook connection (`slack_conn_id`) and the connection to a Postgres database (`postgres_conn_id`) as well and turn on the relevant features via the toggles in `global_variables.py`.
9. If you want to use the DAGs with different data than the sample data from the blog post or the data produced by the `helper_dag_producer` you will need to adjust the `apply_function` and `event_triggered_function` in `listen_to_what_you_say.py`. After your edits make sure to restart Airflow for the new functions to be loaded by the Triggerer Component.
10. Manually run the `helper_dag_producer` to produce messages to your Kafka topic and the `helper_dag_consumer_to_s3` DAG to consume them to your S3 ingest bucket in order to test the pipeline.


DAGs
================

This repository contains the following DAGs:

- `meet_at_s3`: A simple pattern that connects Kafka with Airflow. The DAG runs on a daily schedule and 
    - waits for files to land in an S3 sink
    - runs a decision function (in the example based on the number of files that landed)
    - if the decision condition is fulfilled, trains a SageMaker model
    - moves the files to a storage bucket and deletes the files in the ingest bucket
- `listen_to_what_you_say`: A DAG utilizing the deferrable `EventTriggersFunctionOperator` from the [Kafka Airflow provider](https://github.com/astronomer/airflow-provider-kafka). This DAG will listen to messages in a Kafka topic and based on criteria in the `apply_function`, kick off the `event_triggered_function`. The task will never end and be interrupted by the DAG timing out and restarting every hour. In this example 3 different patterns are detected, one kicks off an extra model training setting via the `extra_model_retrain_dag`, the second one writes data to a Postgres database via the `collect_US_orders_in_db` DAG and the third pattern will post messages to a Slack channel. 

- `extra_model_retrain_dag`: DAG that retrains the provided SageMaker model out of schedule and moves the files used from the ingest to the storage S3 bucket afterwards.
- `collect_US_orders_in_db`: DAG that will collect orders that were posted from the US in a relational database.
- `run_after_model_training_dag`: A DAG that will be kicked off every time the SageMaker model was successfully trained. Posts a message to a Slack channel if configured.

- `helper_dag_producer`: A DAG using the `ProduceToTopicOperator` to quickly produce mock data compatible with the example pipeline to a Kafka topic. 
- `helper_dag_consumer_to_s3`: A DAG using the `ConsumeFromTopicOperator` to quickly consume mock data from the Kafka topic into an S3 bucket.
- `helper_dag_downstream`: An empty DAG for quick testing of downstream dependencies.

Resources
=========
- [Use Apache Kafka with Apache Airflow tutorial](https://docs.astronomer.io/learn/airflow-kafka)
- [Train a machine learning model with SageMaker and Airflow tutorial](https://docs.astronomer.io/learn/airflow-sagemaker) 
- [Write a DAG with the Astro Python SDK tutorial](https://docs.astronomer.io/learn/astro-python-sdk)
- [Kafka provider](https://github.com/astronomer/airflow-provider-kafka)
- [Amazon provider](https://registry.astronomer.io/providers/amazon)
- [Slack provider](https://registry.astronomer.io/providers/slack)
- [Postgres provider](https://registry.astronomer.io/providers/postgres)