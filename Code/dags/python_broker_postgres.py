from airflow import DAG
from tasks_broker_postgres.extract import extract
from tasks_broker_postgres.transformation import transformation
from tasks_broker_postgres.load import load


with DAG(
        dag_id="python_broker_postgres",
        schedule_interval=None,
) as dag:
    extract >> transformation() >> load()
