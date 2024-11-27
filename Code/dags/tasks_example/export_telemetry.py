# ==============================================================================
# Copyright 2024 Sreda Software Solutions. All rights reserved.
# The copyright notice above does not evidence any actual or
# intended publication of such source code. The code contains
# Sreda Software Solutions Confidential Proprietary Information.
# ==============================================================================

"""
Задача получения данных телеметрии поездки.

Этот модуль определяет одну задачу для DAG Airflow,
которая выгружает данные телеметрии поездки.

Атрибуты:
    trip_dates (dict): Словарь, возвращаемый задачей get_dates_for_trip.
    trip_telemetry_data (pandas.DataFrame): набор данных телеметрии поездки.

Функции:
    export_telemetry(**kwargs): выгружает данные телеметрии поездки в определенном интервале.

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.decorators import task
import pandas as pd


@task(task_id="export_telemetry")
def export_telemetry(**kwargs) -> pd.DataFrame:
    """
    Python operator, отвечающий за выгрузку телеметрии поездки

    Returns:
        pd.DataFrame: pandas.DataFrame со следующими атрибутами:
        [timestamp, date, latitude, longitude, altitude, heading, speed, speed_obd \
            acceleration_x, acceleration_y, acceleration_z]
    """

    trip_dates = kwargs['task_instance'].xcom_pull(task_ids='get_dates_for_trip')

    mongo_hook = MongoHook(conn_id="MongoDB")

    # Выгрузка телеметрии по временному интервалу поездки
    trip_telemetry_data = pd.DataFrame(list(mongo_hook.find(
        mongo_collection="da_sd_telemetry",
        query={
            "source_id": trip_dates['source_id'],
            "date": {
                "$gte": trip_dates['trip_start_date'],
                "$lte": trip_dates['trip_stop_date'],
            }
        },
        projection={
            "_id": 0,
            "timestamp":1,
            "date": 1,
            "latitude": "$location.latitude",
            "longitude": "$location.longitude",
            "altitude": "$location.altitude",
            "heading": 1,
            "speed": 1,
            "speed_obd": 1,
            "acceleration_x": "$acceleration.acceleration_x",
            "acceleration_y": "$acceleration.acceleration_y",
            "acceleration_z": "$acceleration.acceleration_z"
        }
    )))

    return trip_telemetry_data
