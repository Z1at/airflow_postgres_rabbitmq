from airflow import DAG
from tasks_broker.sensor import sensor
from tasks_broker.get_data import get_data


with DAG(
        dag_id="python_broker",
        schedule_interval=None,
) as dag:
    sensor >> get_data()
