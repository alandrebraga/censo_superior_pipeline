FROM quay.io/astronomer/astro-runtime:11.6.0

RUN pip install --no-cache-dir soda-core-bigquery soda-core-scientific

RUN python -m venv etl_env && source etl_env/bin/activate && \
    pip install requests==2.28.1 && deactivate

RUN python -m venv dbt_env && source dbt_env/bin/activate && \
    pip install --no-cache-dir google-cloud-bigquery-storage &&\
    pip install --no-cache-dir dbt-bigquery==1.5.3 && deactivate
