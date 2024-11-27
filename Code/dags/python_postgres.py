from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime,timedelta


hook = PostgresHook(postgres_conn_id='Postgres')

request = "insert into public.data (name, birth_date, age) values ('Anton', '2004.11.14', 19)"


def insert():
    hook.run(request)


with DAG(
    dag_id="base_dag",
    default_args={
        "owner": "Kirill",
    }
) as dag:

    t = PythonOperator(
        task_id='insert_postgres',
        python_callable=insert
        )


# request1 = "select * from public.tst2"
# request2 = "insert into public.tst2 (name, birth_date, age) values ('sega', '2004.11.09', 19)"
# request3 = "delete from public.tst2 where age in (19, 20, 24)"
# request4 = "update public.tst2 set age = 20 where age = 15"

# Date in form YYYY-MM-DD

# def get_table():
#     connection = hook.get_conn()
#     cursor = connection.cursor()
#     cursor.execute(request2)
#     # sources = cursor.fetchall()
#     #
#     # print(1)
#     # for source in sources:
#     #     print(source)
#     # return sources
#     # print(2)
#
#
# def insert():
#     hook.run(request2)
#
#
# def update():
#     hook.run(request4)
#
#
# def delete():
#     hook.run(request3)
#
#
# with DAG(
#     dag_id="postgres_get_table",
#     schedule_interval=None,
#     start_date=datetime(2024,7,22),
#     default_args={
#         "owner": "Rob",
#         # "retries": 1,
#         # "retry_delay": timedelta(seconds=10)
#     }
# ) as dag:
#
#     t2 = PythonOperator(
#         task_id='insert_postgres',
#         python_callable=delete
#         )


