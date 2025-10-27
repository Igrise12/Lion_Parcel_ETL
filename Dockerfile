FROM apache/airflow:2.9.3
COPY requirements.txt /
COPY --chown=airflow:root configuration/ configuration/

COPY ./configuration /configuration

RUN pip install --no-cache-dir -r /requirements.txt