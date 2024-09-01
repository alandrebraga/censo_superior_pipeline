
from airflow.decorators import dag
from pendulum import datetime
from include.etl.censo.censo import extract
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.decorators import task
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata
from astro import sql as aql

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "andreb"},
    tags=["dag"],
)
def censo():
    project_name = "andreb_censosuperior"
    bucket_name = "andreb_censosuperior"

    download_censo = extract()

    @task()
    def convert_to_parquet():
        import pandas as pd
        import os

        data_folder = "/usr/local/airflow/include/data"
        csv_files = [file for file in os.listdir(data_folder) if file.endswith('.CSV')]

        for csv_file in csv_files:
            local_file_path = os.path.join(data_folder, csv_file)
            df = pd.read_csv(local_file_path, sep=';',encoding='latin1')

            parquet_file_path = os.path.splitext(local_file_path)[0] + '.parquet'

            df.to_parquet(parquet_file_path, index=False)


    @task
    def upload_to_gcs():
        import os

        data_folder = "/usr/local/airflow/include/data"
        gcs_conn_id = 'gcp'

        csv_files = [file for file in os.listdir(data_folder) if file.endswith('.parquet')]

        for csv_file in csv_files:
            local_file_path = os.path.join(data_folder, csv_file)
            gcs_file_path = f"raw/{csv_file}"

            upload = LocalFilesystemToGCSOperator(
                task_id=f"upload_{csv_file}",
                src=local_file_path,
                dst=gcs_file_path,
                bucket=bucket_name,
                gcp_conn_id=gcs_conn_id,
                mime_type='text/csv'
            )

            upload.execute(context=None)

    @task()
    def remove_data():
        import os
        data_folder = "/usr/local/airflow/include/data"
        for file in os.listdir(data_folder):
            file_path = os.path.join(data_folder, file)
            os.remove(file_path)

    create_censo_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="createdataset_censo",
        dataset_id = "censo_superior",
        gcp_conn_id = "gcp"
    )

    cursos_gcs_to_stg = aql.load_file(
        task_id="curso_gcs_to_stg",
        input_file = File(
            f'gs://{bucket_name}/raw/MICRODADOS_CADASTRO_CURSOS_2022.parquet',
            conn_id='gcp',
            filetype=FileType.PARQUET,
        ),
        output_table = Table(
            name="stg_cursos",
            conn_id="gcp",
            metadata = Metadata(schema="censo_superior"),
        ),
        use_native_support=True,
    )

    ies_gcs_to_stg = aql.load_file(
        task_id="ies_gcs_to_stg",
        input_file = File(
            f'gs://{bucket_name}/raw/MICRODADOS_CADASTRO_IES_2022.parquet ',
            conn_id='gcp',
            filetype=FileType.PARQUET,
        ),
        output_table = Table(
            name="stg_ies",
            conn_id="gcp",
            metadata = Metadata(schema="censo_superior"),
        ),
        use_native_support=True,
    )

    chain(download_censo, convert_to_parquet(),upload_to_gcs(), remove_data(), create_censo_dataset, cursos_gcs_to_stg, ies_gcs_to_stg)

censo()