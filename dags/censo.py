
from airflow.decorators import dag
from pendulum import datetime
from include.etl.censo.censo import extract
from airflow.models.baseoperator import chain

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "andreb"},
    tags=["dag"],
)
def censo():

    download_censo = extract()

    chain(download_censo)

censo()