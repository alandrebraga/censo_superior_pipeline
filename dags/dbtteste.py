from airflow.decorators import dag
from pendulum import datetime
from airflow.models.baseoperator import chain

from cosmos.constants import LoadMode
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.config import ProjectConfig, RenderConfig
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "andreb"},
    tags=["dag"],
)
def dbtteste():
   transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models']
        )
    )

   chain(transform)

dbtteste()