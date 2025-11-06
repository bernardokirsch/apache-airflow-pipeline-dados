FROM apache/airflow:2.9.1-python3.11

USER airflow
RUN pip install pandas requests

ENV PATH="${PATH}:/home/airflow/.local/bin"