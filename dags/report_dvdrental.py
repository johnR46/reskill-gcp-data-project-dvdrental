from airflow.decorators import dag, task
from datetime import datetime
from airflow.utils.helpers import chain
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from cosmos.airflow.task_group import DbtTaskGroup

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dvdrental"],
)
def report_dvdrental():

    report = DbtTaskGroup(
        group_id="report",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS, select=["path:models/dvdrental/report"]
        ),
    )

    @task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
    def check_report():
        from include.soda.check_function import check

        project_root = "include"
        config_file = f"{project_root}/soda/configuration/dvdrental/configuration.yml"
        checks_path = f"{project_root}/soda/checks/dvdrental"
        scan_name = "check_report"
        checks_subpath = "report"
        data_source = "dvdrental"
        return check(config_file, checks_path, scan_name, checks_subpath, data_source)

    chain(report, check_report())


report_dvdrental()
