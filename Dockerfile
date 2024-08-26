FROM quay.io/astronomer/astro-runtime:12.0.0


RUN python -m venv etl_env && source etl_env/bin/activate && \
    pip install requests==2.28.1