from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task(task_id="load")
def load(**kwargs) -> None:
    message = kwargs['task_instance'].xcom_pull(task_ids='transformation')

    hook = PostgresHook(postgres_conn_id='Postgres')

    request = ("insert into public.data (name, birth_date, age) values ('" + message["name"] + "','" +
                                                                            message["birth_date"] + "'," +
                                                                            str(message["age"]) + ")")

    hook.run(request)
