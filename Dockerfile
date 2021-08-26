FROM apache/airflow:2.1.1-python3.8

COPY requirements.txt /opt/airflow/requirements.txt
COPY akshata-project-659fe77ef5f3.json /opt/airflow/akshata-project-158716-659fe77ef5f3.json
RUN pip install -r /opt/airflow/requirements.txt
COPY integrity_test/ /opt/airflow/integrity_test