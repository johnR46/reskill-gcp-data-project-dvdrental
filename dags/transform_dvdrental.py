from airflow.decorators import dag, task
from datetime import datetime
from airflow.utils.helpers import chain
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from cosmos.airflow.task_group import DbtTaskGroup
from pathlib import Path

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dvdrental"],
)
def transform_dvdrental():

    transform = DbtTaskGroup(
        group_id="transform",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS, select=["path:models/dvdrental/transform"]
        ),
    )

    @task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
    def check_transform():
        from include.soda.check_function import check

        project_root = "include"
        config_file = f"{project_root}/soda/configuration/dvdrental/configuration.yml"
        checks_path = f"{project_root}/soda/checks/dvdrental"
        scan_name = "check_transform"
        checks_subpath = "transform"
        data_source = "dvdrental"
        return check(config_file, checks_path, scan_name, checks_subpath, data_source)

    chain(transform, check_transform())


transform_dvdrental()
