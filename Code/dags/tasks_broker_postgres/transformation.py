import json
from airflow.decorators import task


@task(task_id="transformation")
def transformation(**kwargs) -> dict:
    message = json.loads(kwargs['task_instance'].xcom_pull(task_ids='extract'))

    new_message = {"name": message["name"],
                   "birth_date": message["birth_date"],
                   "age": message["age"]}

    return new_message
