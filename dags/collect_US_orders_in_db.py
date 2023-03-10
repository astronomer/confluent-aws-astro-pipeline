"""This DAG uses the Astro SDK to write messages with US orders into a separate
Postgres database."""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.empty import EmptyOperator
# import SDK packages
import astro.sql as aql
from astro.files import File
from astro.sql.table import Table

import json
import os
import logging

# import variables
from include import global_variables as gv

# get Airflow task logger
log = logging.getLogger('airflow.task')


@dag(
    start_date=datetime(2023, 1, 23),
    schedule=None,
    catchup=False,
    params={
        "message": None
    },
    render_template_as_native_obj=True,
    tags=["listen_pattern"]
)
def collect_US_orders_in_db():

    @task(
        templates_dict={
            "message": "{{ params.message }}",
            "run_id": "{{ run_id }}"
        }
    )
    def write_tmp_message(**kwargs):
        """This task writes the message to a local temporary JSON file.
        Additionally it adds an 'index' parameter with the logical date
        timestamp of this DAG."""

        run_id = kwargs["templates_dict"]["run_id"]
        message = kwargs["templates_dict"]["message"]

        with open("tmp.json", 'w') as f:
            json.dump(
                {
                    "index": [run_id],
                    **{k: [v] for k, v in message.items()}
                }, f
            )
        log.info(f"Wrote {kwargs['templates_dict']['message']} to tmp file.")

    write_msg = write_tmp_message()

    if gv.WRITE_MSG_TO_DB:

        # Astro SDK task to load the data from the local JSON file to
        # a Postgres database
        load_file_sdk = aql.load_file(
            input_file=File(path="tmp.json"),
            output_table=Table(
                name=gv.DB_NAME,
                conn_id=gv.POSTGRES_CONN,
            ),
        )

    else:

        # Empty task for pipeline testing without db interaction
        load_file_sdk = EmptyOperator(
            task_id="load_file_sdk"
        )

    @task
    def delete_tmp_message():
        """This task deletes the temporary local JSON file."""
        os.remove("tmp.json")

    write_msg >> load_file_sdk >> delete_tmp_message()


collect_US_orders_in_db()
