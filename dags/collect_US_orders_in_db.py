"""This DAG uses the Astro SDK to write messages with US orders into a separate
Postgres database.
"""

from airflow.decorators import dag, task
from pendulum import datetime
# import SDK packages
import astro.sql as aql
from astro.files import File
from astro.sql.table import Table
import json
import os
import logging

# import variables
from include import global_variables as gv

log = logging.getLogger('airflow.task')

@dag(
    start_date=datetime(2023, 1, 23),
    schedule_interval=None,
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
            "message" : "{{ params.message }}",
            "ts": "{{ ts }}"
        }
    )
    def write_tmp_message(**kwargs):
        """This task writes the message to a local temporary JSON file.
        Additionally it adds an 'index' parameter with the logical date
        timestamp of this DAG."""

        timestamp = kwargs["templates_dict"]["ts"]
        message = kwargs["templates_dict"]["message"]

        with open("tmp.json", 'w') as f:
                json.dump(
                    {
                        "index": [timestamp],
                        **{k:[v] for k,v in message.items()}
                    }
                    , f
                )
        log.info(f"Wrote {kwargs['templates_dict']['message']} to tmp file.")

    write_msg = write_tmp_message()

    # Astro SDK task to load the data from the local JSON file to a Postgres
    # Database
    load_file_sdk =  aql.load_file(
        input_file=File(path="tmp.json"),
        output_table=Table(
            name="US_ORDERS",
            conn_id=gv.POSTGRES_CONN,
        ),
    )

    @task
    def delete_tmp_message():
        """This task deletes the temporary local JSON file."""
        os.remove("tmp.json")

    write_msg >> load_file_sdk >> delete_tmp_message()

collect_US_orders_in_db()