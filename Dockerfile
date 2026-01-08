FROM apache/airflow:3.1.5
RUN pip install --no-cache-dir debugpy minio
COPY dags /opt/airflow/dags
