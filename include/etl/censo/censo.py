from airflow.decorators import task

@task.external_python(python='/usr/local/airflow/etl_env/bin/python')
def extract():
    import shutil
    import requests
    import zipfile
    from io import BytesIO
    import urllib3
    import os

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    BASE_URLS: str = (
    "http://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2022.zip",
    )
    def download_data_and_unzip(urls):
        for url in urls:
            try:
                filebytes = BytesIO(requests.get(url, verify=False).content)
                zipfile.ZipFile(filebytes).extractall("/usr/local/airflow/include/etl/censo/files")
                print(f"Successfully downloaded: {url}")
            except requests.exceptions.RequestException as e:
                raise Exception(f"Failed to download {url}: {e}")

    def move_data_to_folder():
        dest_dir = "/usr/local/airflow/include/data"
        source_dir = "/usr/local/airflow/include/etl/censo/files"

        os.makedirs(dest_dir, exist_ok=True)

        for dirpath, dirnames, filenames in os.walk(source_dir):
            for file in filenames:
                if file.endswith(".CSV"):
                    source_path = os.path.join(dirpath, file)
                    dest_path = os.path.join(dest_dir, file)

                    if os.path.exists(dest_path):
                        base, extension = os.path.splitext(file)
                        counter = 1
                        while os.path.exists(dest_path):
                            new_file = f"{base}_{counter}{extension}"
                            dest_path = os.path.join(dest_dir, new_file)
                            counter += 1

                    shutil.move(source_path, dest_path)
                    print(f"Successfully moved: {file} to {dest_path}")

        shutil.rmtree(source_dir)

        old_name = os.path.join(dest_dir, "MICRODADOS_ED_SUP_IES_2022.CSV")
        new_name = os.path.join(dest_dir, "MICRODADOS_CADASTRO_IES_2022.CSV")

        if os.path.exists(old_name):
            os.rename(old_name, new_name)
            print(f"Renamed {old_name} to {new_name}")
        else:
            print(f"File {old_name} not found for renaming")

    download_data_and_unzip(BASE_URLS)
    move_data_to_folder()

if __name__ == '__main__':
    extract()