# ==============================================================================
# Copyright 2024 Sreda Software Solutions. All rights reserved.
# The copyright notice above does not evidence any actual or
# intended publication of such source code. The code contains
# Sreda Software Solutions Confidential Proprietary Information.
# ==============================================================================

"""
Задача для формирования документа поездки.

Этот модуль определяет одну задачу для DAG Airflow, которая собирает
информацию из коллекций MongoDB и возвращаемых значений предыдущих задач
для формирования документа поездки.

Атрибуты:
    dates_for_trip (dict): Словарь с данными о начале/конце поездки.
    map_matched_telemetry_df (pandas.DataFrame): Набор данных телеметрии, привязанных к дорожному графу, возвращаемый задачей map_match.
    maneuvers_df (list[dict]): Список из маневров формата feature geojson, возвращаемых задачей get_maneuvers.
    da_sd_app_events (pandas.DataFrame): Набор данных событий поездки из коллекции da_sd_app_events.
    da_sd_obd (pandas.DataFrame): Набор данных событий поездки из коллекции da_sd_obd.
    da_sd_trips (pandas.DataFrame): Набор данных событий поездки из коллекции da_sd_trips.

Функции:
    find_docs(mongo_hook: MongoHook, collection_name: str, trip_data: dict): Выгружает данные из коллекции MongoDB и возвращает в виде pd.DataFrame.
    get_changing_10(da_sd_obd: pd.DataFrame, event_type: str): Вычисляет количество заправок и перезарядок.
    overspeeding_rate(da_sd_speeding: pd.DataFrame): Вычисляет процентное отношение по классам превышения скоростного режима.
    get_stops(da_sd_telemetry: pd.DataFrame): Вычисляет время простоя ТС за всю поездку на основе телеметрии.
    get_trip_distance(telemetry_df: pd.DataFrame): Вычисляет дистанцию поездки.
    detect_last_data(obd_data: pd.DataFrame, trips_data: pd.DataFrame, dist: float, dur: float): Вычисляет актуальные данные о пробеге и времени работы двигателя.

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""
from typing import List, Tuple
from datetime import datetime, timezone

import pandas as pd
import numpy as np
import geopandas as gpd
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.decorators import task
from shapely.geometry import LineString


def find_docs(mongo_hook: MongoHook, collection_name: str, trip_data: dict) -> pd.DataFrame:
    """
    Get docs from MongoDB collection

    Args:
        mongo_hook: airflow MongoHook.
        collection_name: collection name from which data is taken.
        trip_data: DataFrame with trip info (source_id, trip_start, trip_stop).

    Returns:
        docs in form of DataFrame
    """
    df = pd.DataFrame(
        list(
            mongo_hook.find(
                mongo_collection=collection_name,
                query={
                    "source_id": trip_data['source_id'],
                    '$and': [
                        {"date": {'$gte': trip_data['trip_start_date']}},
                        {"date": {'$lte': trip_data['trip_stop_date']}}
                    ]
                }
            ).sort("date")
        )
    )

    print(f"Get {len(df)} documents in {collection_name} collection")
    return df


def get_changing_10(da_sd_obd: pd.DataFrame, event_type: str) -> int:
    """
    Вычисляет количество заправок и перезарядок, где значение изменяется более, чем на 10%.

    Атрибуты:
        da_sd_obd (pd.DataFrame): Набор данных с событиями obd.
        event_type (str): Тип события.

    Возвращаемое значение:
        refills (int): количество заправок/перезарядок.
    """
    if da_sd_obd.empty:
        return 0
    data = da_sd_obd[da_sd_obd['event_type'] == event_type]
    changes = data['value'].diff()
    refills = int((changes > 10).sum())
    return refills


def overspeeding_rate(da_sd_speeding: pd.DataFrame) -> List[float]:
    """
    Вычисляет процентное отношение по классам превышения скоростного режима.

    Атрибуты:
        da_sd_speeding: Набор данных телеметрии с привязанными типами нарушения скоростного режима.

    Возвращаемое значение:
        list[float]: Список из 4 значений.
    """
    overspeeding = [0, 0, 0, 0]
    quantity = len(da_sd_speeding)

    if quantity == 0:
        return overspeeding

    class_counts = da_sd_speeding["overspeed_class"].value_counts().sort_index()
    overspeeding = [np.round(class_counts.get(x, 0) / quantity, 2) for x in range(4)]

    return overspeeding


def get_stops(da_sd_telemetry: pd.DataFrame) -> int:
    """
    Вычисляет время простоя ТС за всю поездку на основе телеметрии.

    Атрибуты:
        da_sd_telemetry: Набор данных телеметрии.

    Возвращаемое значение:
        int: Время простоя ТС в секундах.
    """

    def calculate_stop_time(row):
        nonlocal start_stop
        nonlocal total_stop_time

        # Начало простоя
        if row['speed_obd'] == 0 and (start_stop is None or start_stop != 0):
            start_stop = row['date']

        # Конец простоя
        elif row['speed_obd'] != 0 and start_stop is not None and start_stop == 0:
            total_stop_time += (row['date'] - start_stop).total_seconds()
            start_stop = None

    start_stop = None
    total_stop_time = 0

    da_sd_telemetry.apply(calculate_stop_time, axis=1)

    # If the stop continues until the end of the DataFrame, add the time to the end
    if start_stop == 0:
        total_stop_time += (da_sd_telemetry['date'].max() - start_stop).total_seconds()

    return int(total_stop_time)


def get_trip_distance(telemetry_df: pd.DataFrame) -> float:
    """
    Вычисляет дистанцию поездки на основе телеметрии.

    Атрибуты:
        da_sd_telemetry: Набор данных телеметрии.

    Возвращаемое значение:
        trip_distance (float): Дистанция поездки в метрах.
    """
    telemetry_gdf = gpd.GeoDataFrame(
        telemetry_df,
        geometry=gpd.points_from_xy(x=telemetry_df['longitude'], y=telemetry_df['latitude']),
        crs="EPSG:4326"
    )

    trip_distance = round(LineString(
        telemetry_gdf['geometry'].to_crs(telemetry_gdf.estimate_utm_crs())
    ).length, 2)

    return trip_distance


def detect_last_data(obd_data: pd.DataFrame, trips_data: pd.DataFrame, dist: float, dur: float):
    """
    Вычисляет актуальные данные о пробеге и времени работы двигателя.

    Атрибуты:
        obd_data (pd.DataFrame): Набор данных с событиями obd.
        trips_data (pd.DataFrame): Набор данных о предыдущих поездках. 
        dist (float): Дистанция текущей поездки.
        dur (float): Длительность текущей поездки.

    Возвращаемое значение:
        last_mileage: Актуальный пробег автомобиля в метрах.
        last_engine_run_time: Актуальное время работы двигателя в секундах.
    """
    if obd_data.empty:
        last_mileage = None
        last_engine_run_time = None
    else:
        mileage_data = obd_data[obd_data["event_type"] == "mileage"]
        engine_run_time_data = obd_data[obd_data["event_type"] == "engine_run_time"]

        if mileage_data.empty:
            last_mileage = int(mileage_data["value"].max())
        elif not trips_data.empty:
            last_mileage = int(trips_data["stat"]["mileage"].max() + (dist / 1000))
        else:
            last_mileage = None

        if not engine_run_time_data.empty:
            last_engine_run_time = int(engine_run_time_data["value"].max())
        elif not trips_data.empty:
            last_engine_run_time = int(trips_data["stat"]["engine_run_time"].max() + (dur / 3600))
        else:
            last_engine_run_time = None

    return last_mileage, last_engine_run_time


@task(task_id="make_trip")
def make_trip(**kwargs):
    """
    Python Operator, обеспечивающий формирование документа поездки и его запись в базу MongoDB.

    Возвращаемое значение:
        trip_record (dict): Документ поездки в виде словаря python, следующей структуры:

        {
            "schema_version": 1, //(int) Версия схемы данных.
            "source_id": "source_id", //(str) Идентификатор ТС.
            "detector_id": 1, //(int) Идентификатор операционной системы мобильного приложения.
            "timestamp": 1719481188030, //(int) Временная метка формирования документа поездки в мс.
            "date": "2024-06-25T14:27:50.632+0000", //(datetime.datetime) Дата формирования документа поездки.
            "start_date": "2024-06-25T14:27:50.632+0000", //(datetime.datetime) Дата начала поездки.
            "end_date": 2024-06-25T15:27:50.632+0000, //(datetime.datetime) Дата окончания поездки.
            "geojson": { // Geojson с точками начала/окончания поездки, телеметрией поездки в виде LineString, и features с маневрами
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "name": "trip_start_point"
                        },
                        "geometry": {
                            "type": "Point",
                            "coordinates": [30.398084, 59.915392]
                        }
                    },
                    {
                        "type": "Feature",
                        "properties": {
                            "name": "trip_stop_point"
                        },
                        "geometry": {
                            "type": "Point",
                            "coordinates": [30.39899, 59.902701]
                        }
                    },
                    {
                        "type": "Feature",
                        "properties": {
                            "name": "trip_line"
                        },
                        "geometry": {
                            "type": "LineString",
                            "coordinates": [[30.398084, 59.915392], [30.398084, 59.915392], [30.398084, 59.915392]]
                        }
                    },
                    {
                        "type" : "Feature",
                        "geometry" : {
                            "type" : "Point",
                            "coordinates" : [
                                30.397934,
                                59.902148
                            ]
                        },
                        "properties" : {
                            "timestamp" : NumberLong(1717749159270),
                            "date" : ISODate("2024-06-07T08:32:39.270+0000"),
                            "event_type" : "right_turn",
                            "duration" : 6.101,
                            "from_way" : NumberInt(541160376),
                            "to_way" : NumberInt(623177137),
                            "angle" : 275.032035,
                            "speed" : {
                                "from" : 31.211714,
                                "to" : 11.746365,
                                "max" : 31.211714,
                                "min" : 11.746365,
                                "mean" : 26.94685452830189,
                                "q50" : 28.65658,
                                "q25" : 23.190973,
                                "q75" : 30.600311
                            },
                            "dangerous" : false
                        }
                    }
                ]
            },
            "stat": {
                "distance": 1813.93, //Дистанция поездки в метрах.
                "duration": 21016.26, //Длительность поездки в секундах.
                "hard_accelerations": 20, //Количество резких ускорений.
                "hard_braking": 20, //Количество резких ускорений.
                "sharp_maneuvers": 0, //Количество резких маневров.
                "overspeeding": [1.0, 0.0, 0.0, 0.0], //Процентное отношение по классам превышения скоростного режима.
                "stops": 0, //Время простоя ТС в секундах.
                "fuel_refills": 0, //Количество заправок.
                "battery_pack_refills": 0, //Количество перезарядок.
                "mileage": 10000, //Пробег ТС.
                "engine_run_time": 10000 //Время работы двигателя ТС.
            }
        }
    """
    mongo_hook = MongoHook(conn_id="MongoDB")

    dates_for_trip = kwargs["task_instance"].xcom_pull(task_ids="get_dates_for_trip")
    map_matched_telemetry_df = kwargs["task_instance"].xcom_pull(task_ids="map_match")
    maneuvers_df = kwargs["task_instance"].xcom_pull(task_ids="get_maneuvers")

    current_datetime = datetime.now(timezone.utc)

    da_sd_app_events = find_docs(mongo_hook, "da_sd_app_events", dates_for_trip)
    da_sd_obd = find_docs(mongo_hook, "da_sd_obd", dates_for_trip)
    da_sd_trips = find_docs(mongo_hook, "da_sd_trips", dates_for_trip)

    distance = get_trip_distance(map_matched_telemetry_df)
    duration = round((dates_for_trip['trip_stop_date'] - dates_for_trip['trip_start_date']).total_seconds(), 2)

    mileage, engine_runtime = detect_last_data(da_sd_obd, da_sd_trips, distance, duration)

    trip_record = {
        "schema_version": 1,
        "source_id": dates_for_trip['source_id'],
        "detector_id": dates_for_trip['detector_id'],
        "timestamp": int(current_datetime.timestamp() * 1000),
        "date": current_datetime,
        "start_date": dates_for_trip['trip_start_date'],
        "end_date": dates_for_trip['trip_stop_date'],
        "geojson": {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "name": "trip_start_point"
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            map_matched_telemetry_df.iloc[0]['longitude'],
                            map_matched_telemetry_df.iloc[0]['latitude']
                        ]
                    }
                },
                {
                    "type": "Feature",
                    "properties": {
                        "name": "trip_stop_point"
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            map_matched_telemetry_df.iloc[-1]['longitude'],
                            map_matched_telemetry_df.iloc[-1]['latitude']
                        ]
                    }
                },
                {
                    "type": "Feature",
                    "properties": {
                        "name": "trip_line"
                    },
                    "geometry": {
                        "type": "LineString",
                        "coordinates": map_matched_telemetry_df[['longitude', 'latitude']].values.tolist()
                    }
                }
            ] + maneuvers_df
        },
        "stat": {
            "distance": distance,
            "duration": duration,
            "hard_accelerations": len(da_sd_app_events[da_sd_app_events["event_type"] == "acceleration_event"]),
            "hard_braking": len(da_sd_app_events[da_sd_app_events["event_type"] == "braking_event"]),
            "sharp_maneuvers": len([maneuver for maneuver in maneuvers_df if maneuver['properties']["dangerous"]]),
            "overspeeding": overspeeding_rate(kwargs["task_instance"].xcom_pull(task_ids="detect_speedings")),
            "stops": get_stops(kwargs["task_instance"].xcom_pull(task_ids="export_telemetry")),
            "fuel_refills": get_changing_10(da_sd_obd, "fuel_level"),
            "battery_pack_refills": get_changing_10(da_sd_obd, "battery_pack_level"),
            "mileage": mileage,
            "engine_run_time": engine_runtime
        }
    }

    mongo_hook.insert_one(
        mongo_collection="da_sd_trips",
        doc=trip_record
    )

    del trip_record['_id']

    return trip_record
