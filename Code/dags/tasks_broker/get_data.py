import json
from airflow.decorators import task


@task(task_id="get_data")
def get_data(**kwargs) -> None:
    message = json.loads(kwargs['task_instance'].xcom_pull(task_ids='sensor'))

    print("#########################################################################################")
    print(message)
    print("#########################################################################################")

