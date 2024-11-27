# ==============================================================================
# Copyright 2024 Sreda Software Solutions. All rights reserved.
# The copyright notice above does not evidence any actual or
# intended publication of such source code. The code contains
# Sreda Software Solutions Confidential Proprietary Information.
# ==============================================================================

"""
Задача получения временного интервала поездки.

Этот модуль определяет одну задачу для DAG Airflow,
которая определяет временной интервал поездки и формирует документ о начале и конце поездки.

Атрибуты:
    trip_stop_message (dict): Сообщение от задачи trip_stop_sensor.
    trip_dates (dict): Словарь с данными о начале/конце поездки.

Функции:
    get_dates_for_trip(**kwargs): определяет временной интервал поездки.

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""

import json
from datetime import datetime

# from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.decorators import task


@task(task_id="get_dates_for_trip")
def get_dates_for_trip(**kwargs) -> dict:
    """
    Python operator, отвечающий за выгрузку данных о временном интервале поездки.

    Returns:
        dict: словарь формата:
        {
            "source_id": "1", //str
            "detector_id: 1 //int
            "trip_stop_date": "2020-06-26T12:29:01.000", //python datetime object
            "trip_start_date": "2020-06-26T10:29:01.000" //python datetime object
        }
    """

    trip_stop_message = json.loads(kwargs['task_instance'].xcom_pull(task_ids='trip_stop_sensor'))

    print("#########################################################################################")
    print(trip_stop_message)
    print("#########################################################################################")
    # mongo_hook = MongoHook(conn_id="MongoDB")

    # Находим последнее по времени событие trip_start_event
    # Поле date не должно превышать по времени trip_stop_date
    # trip_start_message = list(mongo_hook.find(
    #     mongo_collection="da_sd_app_events",
    #     query={
    #         "event_type": "trip_start_event",
    #         "source_id": trip_stop_message['source_id'],
    #         "date": {"$lt": datetime.fromisoformat(trip_stop_message['trip_stop_date'])}
    #     }
    # ).sort({"date": -1}).limit(1))[0]

    # Формируем выходной документ
    # trip_dates = {
    #     "source_id": trip_stop_message["source_id"],
    #     "detector_id": trip_start_message["detector_id"],
    #     "trip_stop_date": datetime.fromisoformat(trip_stop_message['trip_stop_date']),
    #     "trip_start_date": trip_start_message["date"]
    # }
    # return trip_dates
