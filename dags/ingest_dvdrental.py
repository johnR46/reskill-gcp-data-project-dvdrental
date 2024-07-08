from airflow.decorators import dag, task
from datetime import datetime

# from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
# from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

# from astro import sql as aql
# from astro.files import File
# from astro.sql.table import Table, Metadata
# from astro.constants import FileType

# # from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
# from cosmos.airflow.task_group import DbtTaskGroup
# from cosmos.constants import LoadMode
# from cosmos.config import ProjectConfig, RenderConfig

from airflow.utils.helpers import chain
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['dvdrental'],
    template_searchpath=[
        "/usr/local/airflow/include/dataset/dvdrental/extract"
    ]
)
def ingest_dvdrental():

    project_id="deep-equator-427111-t7"
    
    dt_now = datetime.now()
    dt_year = dt_now.year
    dt_month = dt_now.strftime("%Y%m")
    dt_day = dt_now.strftime("%Y%m%d")
    MY_BUCKET  = f"my-lake-dvdrental"

    upload_address_to_gcs = PostgresToGCSOperator(
        task_id="upload_address_to_gcs",
        sql="address.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/address-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_city_to_gcs = PostgresToGCSOperator(
        task_id="upload_city_to_gcs",
        sql="city.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/city-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_country_to_gcs = PostgresToGCSOperator(
        task_id="upload_country_to_gcs",
        sql="country.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/country-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_customer_to_gcs = PostgresToGCSOperator(
        task_id="upload_customer_to_gcs",
        sql="customer.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/customer-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_film_to_gcs = PostgresToGCSOperator(
        task_id="upload_film_to_gcs",
        sql="film.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/film-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_inventory_to_gcs = PostgresToGCSOperator(
        task_id="upload_inventory_to_gcs",
        sql="inventory.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/inventory-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_language_to_gcs = PostgresToGCSOperator(
        task_id="upload_language_to_gcs",
        sql="language.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/language-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_payment_to_gcs = PostgresToGCSOperator(
        task_id="upload_payment_to_gcs",
        sql="payment.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/payment-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_staff_to_gcs = PostgresToGCSOperator(
        task_id="upload_staff_to_gcs",
        sql="staff.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/staff-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_store_to_gcs = PostgresToGCSOperator(
        task_id="upload_store_to_gcs",
        sql="store.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/store-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    upload_rental_to_gcs = PostgresToGCSOperator(
        task_id="upload_rental_to_gcs",
        sql="rental.sql",
        bucket=MY_BUCKET,
        filename=f"{dt_year}/{dt_month}/{dt_day}/rental-{dt_day}",
        postgres_conn_id="pg_dvdrental_conn",
        export_format="csv",
        gcp_conn_id="gcp_conn" 
    )
    # create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
    #     task_id='create_retail_dataset',
    #     dataset_id='retail',
    #     gcp_conn_id='gcp',
    # )
    # gcs_to_raw = aql.load_file(
    #     task_id='gcs_to_raw',
    #     input_file=File(
    #         'gs://reskill_online_retail/raw/online_retail.csv',
    #         conn_id='gcp',
    #         filetype=FileType.CSV,
    #     ),
    #     output_table=Table(
    #         name='raw_invoices',
    #         conn_id='gcp',
    #         metadata=Metadata(schema='retail')
    #     ),
    #     use_native_support=False,
    # )

    # @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    # def check_load(scan_name='check_load', checks_subpath='sources'):
    #     from include.soda.check_function import check
    #     return check(scan_name, checks_subpath)

    # @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    # def check_transform(scan_name='check_transform', checks_subpath='transform'):
    #     from include.soda.check_function import check
    #     return check(scan_name, checks_subpath)

    # @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    # def check_report(scan_name='check_report', checks_subpath='report'):
    #     from include.soda.check_function import check
    #     return check(scan_name, checks_subpath)

    # transform = DbtTaskGroup(
    #     group_id='transform',
    #     project_config=DBT_PROJECT_CONFIG,
    #     profile_config=DBT_CONFIG,
    #     render_config=RenderConfig(
    #         load_method=LoadMode.DBT_LS,
    #         select=['path:models/transform']
    #     )
    # )
    # report = DbtTaskGroup(
    #     group_id='report',
    #     project_config=DBT_PROJECT_CONFIG,
    #     profile_config=DBT_CONFIG,
    #     render_config=RenderConfig(
    #         load_method=LoadMode.DBT_LS,
    #         select=['path:models/report']
    #     )
    # )

    chain(
       
            upload_address_to_gcs,
            upload_city_to_gcs,
    
            upload_country_to_gcs,
            upload_customer_to_gcs,
    
            upload_film_to_gcs,
            upload_inventory_to_gcs,
       
            upload_language_to_gcs,
            upload_payment_to_gcs,
        
            upload_staff_to_gcs,
            upload_store_to_gcs,

            upload_rental_to_gcs
        
        # upload_to_gcs,
        # create_retail_dataset,
        # gcs_to_raw,
        # check_load(),
        # transform,
        # check_transform(),
        # report,
        # check_report()
    )


ingest_dvdrental()
