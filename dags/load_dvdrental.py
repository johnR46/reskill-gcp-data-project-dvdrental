from airflow.decorators import dag, task
from airflow.utils.helpers import chain
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['dvdrental'],
)
def load_dvdrental():

    project_id = "deep-equator-427111-t7"
    dt_now = datetime.now()
    dt_year = dt_now.year
    dt_month = dt_now.strftime("%Y%m")
    dt_day = dt_now.strftime("%Y%m%d")
    MY_BUCKET = f"my-lake-dvdrental"

    create_dvdrental_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dvdrental_dataset',
        dataset_id='dvdrental',
        gcp_conn_id='gcp_conn',
    )

    gcs_address_to_raw = GCSToBigQueryOperator(
        task_id="gcs_address_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/address-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_address",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_city_to_raw = GCSToBigQueryOperator(
        task_id="gcs_city_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/city-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_city",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_country_to_raw = GCSToBigQueryOperator(
        task_id="gcs_country_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/country-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_country",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_customer_to_raw = GCSToBigQueryOperator(
        task_id="gcs_customer_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/customer-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_customer",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_film_to_raw = GCSToBigQueryOperator(
        task_id="gcs_film_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/film-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_film",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_inventory_to_raw = GCSToBigQueryOperator(
        task_id="gcs_inventory_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/inventory-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_inventory",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_language_to_raw = GCSToBigQueryOperator(
        task_id="gcs_language_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/language-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_language",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_payment_to_raw = GCSToBigQueryOperator(
        task_id="gcs_payment_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/payment-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_payment",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_staff_to_raw = GCSToBigQueryOperator(
        task_id="gcs_staff_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/staff-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_staff",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_store_to_raw = GCSToBigQueryOperator(
        task_id="gcs_store_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/store-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_store",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    gcs_rental_to_raw = GCSToBigQueryOperator(
        task_id="gcs_rental_to_raw",
        bucket=MY_BUCKET,
        source_objects=f"{dt_year}/{dt_month}/{dt_day}/rental-{dt_day}",
        destination_project_dataset_table=f"{project_id}.dvdrental.raw_rental",
        gcp_conn_id="gcp_conn",
        write_disposition="WRITE_TRUNCATE"
    )
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load():
        from include.soda.check_function import check
        project_root = "include"
        config_file = f'{project_root}/soda/configuration/dvdrental/configuration.yml'
        checks_path = f'{project_root}/soda/checks/dvdrental'
        scan_name = "check_load"
        checks_subpath = 'sources'
        data_source = "dvdrental"
        return check(config_file, checks_path, scan_name, checks_subpath, data_source)

    chain(
        create_dvdrental_dataset,
        gcs_address_to_raw,
        gcs_city_to_raw,
        gcs_country_to_raw,
        gcs_customer_to_raw,
        gcs_film_to_raw,
        gcs_inventory_to_raw,
        gcs_language_to_raw,
        gcs_payment_to_raw,
        gcs_staff_to_raw,
        gcs_store_to_raw,
        gcs_rental_to_raw,
        check_load()
    )


load_dvdrental()
