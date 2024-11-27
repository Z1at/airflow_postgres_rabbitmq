# ==============================================================================
# Copyright 2024 Sreda Software Solutions. All rights reserved.
# The copyright notice above does not evidence any actual or
# intended publication of such source code. The code contains
# Sreda Software Solutions Confidential Proprietary Information.
# ==============================================================================

import datetime
from typing import Union
import numpy as np
import pymongo

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.decorators import task


def find_user_id_postgres(postgres_hook: PostgresHook, source_id: str, logger) -> str:
    """
    Get docs from PostgresSQL table

    Args:
        postgres_hook: airflow PostgresHook.
        source_id: auto id.
        logger: logger.

    Returns:
        docs in form of list
    """
    # dev use vehicle.vehicle table
    postgres_table_name = "vehicle.vehicle"

    # Получаем данные из Постгреса в формате list
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(f"SELECT user_id, id FROM {postgres_table_name} WHERE public_id = '{source_id}'")
    data = cursor.fetchall()

    # Может не оказаться юзер айди в Постгресе, поэтому надо присвоить пустую строку, чтобы не сломать дальнейший поиск в Монге
    if not data:
        logger.info(f"There are no documents with source_id equal to "
                         f"{source_id} in {postgres_table_name} postgres table")

        return ""

    logger.info(f"Get {len(data)} documents in {postgres_table_name} postgres table")

    # Возвращаем юзер айди
    return str(data[0][0])


def find_mongo_docs(
        mongo_hook: MongoHook,
        user_id_value: str,
        current_date: datetime.datetime,
        logger
) -> list:
    """
    Get docs from MongoDB collection

    Args:
        mongo_hook: airflow MongoHook.
        user_id_value: user id.
        current_date: current time in UTC.
        logger: logger.

    Returns:
        docs in form of list
    """
    collection_name = "da_sd_stat"

    # Берем данные за текущий месяц, поэтому меняем day в current_date на 1
    time_filter = {
        "date": {
            "$gte": current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
            "$lte": current_date
        }
    }

    data = list(
        mongo_hook.find(
            mongo_collection=collection_name,
            query={
                "$and":
                    [
                        {"user_id": user_id_value},
                        time_filter
                    ]
            },
            projection={"_id": 0}
        ).sort("date", pymongo.DESCENDING).limit(1)
    )

    logger.info(f"Get {len(data)} documents in {collection_name} collection")

    return data


def calculate_statistics(trip_data: dict, postgres_hook: PostgresHook, mongo_hook: MongoHook, logger) -> None:
    """
    Calculate statistics and scores based on generated trip and last actual user stat and insert it into DB.

    Args:
        trip_data: dict with trip data.
        postgres_hook: airflow Postgres.
        mongo_hook: airflow MongoHook.
        logger: logger.
    """
    source_id = trip_data["source_id"]
    # print(source_id, 'kkkkkkkkkkkkkkkkkkkkkkkkkkk')
    user_id_value = find_user_id_postgres(postgres_hook, source_id, logger)

    current_time_in_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    current_date = datetime.datetime.fromtimestamp(current_time_in_timestamp)

    da_sd_stat = find_mongo_docs(mongo_hook, user_id_value, current_date, logger)

    # Score статистики считается по следующим правилам
    acc_score = 25 - int(25 * trip_data["stat"]['hard_accelerations'] / trip_data["stat"]['distance'])
    brake_score = 25 - int(25 * trip_data["stat"]['hard_braking'] / trip_data["stat"]['distance'])
    maneuvers_score = 25 - int(25 * trip_data["stat"]['sharp_maneuvers'] / trip_data["stat"]['distance'])

    # Считаем количество превышений скорости
    overspeeding = sum(trip_data["stat"]['overspeeding'][1:])

    if user_id_value == "":
        logger.error(f"There is no user who has car: {trip_data['source_id']}")
        return

    if not len(da_sd_stat):
        logger.info(
            "Empty list (da_sd_stat)!\
            There are no documents corresponding to this calendar month."
        )
        # Считаем score скорости, учитывая количество превышений скорости
        speed_score = 25 - int(25 * overspeeding)

        # Получаем общий score исходя из всех правил
        total_score = acc_score + brake_score + maneuvers_score + speed_score

        record = {
            "schema_version": trip_data["schema_version"],
            "user_id": user_id_value,
            "date": current_date,
            "trips": 1,
            "stops": trip_data["stat"]['stops'],
            "fuel_refills": trip_data["stat"]['fuel_refills'],
            "battery_pack_refills": trip_data["stat"]['battery_pack_refills'],
            "total_distance": trip_data["stat"]['distance'],
            "total_duration": trip_data["stat"]['duration'],
            "hard_accelerations": trip_data["stat"]['hard_accelerations'],
            "hard_braking": trip_data["stat"]['hard_braking'],
            "sharp_maneuvers": trip_data["stat"]['sharp_maneuvers'],
            "overspeeding": trip_data["stat"]['overspeeding'],
            "vehicle_stat": [
                {
                    "source_id": source_id,
                    "mileage": trip_data["stat"]['mileage'],
                    "engine_run_time": trip_data["stat"]['engine_run_time']
                }
            ],
            "score": total_score
        }
        da_sd_stat = record
    else:
        # Получаем единственный документ из списка
        da_sd_stat = da_sd_stat[0]

        # Инкрементируем количество поездок
        trips = da_sd_stat["trips"] + 1

        # Считаем score скорости, учитывая количество превышений скорости и количество поездок
        speed_score = 25 - int(25 * overspeeding / trips)

        # Получаем общий score как сумму всех правил и предыдущего общего score. Надо поделить сумму на 2
        total_score = int(np.round((acc_score + brake_score + maneuvers_score + speed_score + da_sd_stat["score"]) / 2))

        # Обновляем данные статистики
        da_sd_stat["date"] = current_date
        da_sd_stat["trips"] = trips
        da_sd_stat["stops"] += trip_data["stat"]['stops']
        da_sd_stat["fuel_refills"] += trip_data["stat"]['fuel_refills']
        da_sd_stat["battery_pack_refills"] += trip_data["stat"]['battery_pack_refills']

        current_stat_total_distance = da_sd_stat['total_distance']

        da_sd_stat["total_distance"] += trip_data["stat"]['distance']
        da_sd_stat["total_duration"] += trip_data["stat"]['duration']
        da_sd_stat["hard_accelerations"] += trip_data["stat"]['hard_accelerations']
        da_sd_stat["hard_braking"] += trip_data["stat"]['hard_braking']
        da_sd_stat["sharp_maneuvers"] += trip_data["stat"]['sharp_maneuvers']

        stat_total_distance = da_sd_stat['total_distance']

        stat_distance_coef = current_stat_total_distance / stat_total_distance
        current_trip_distance_coef = trip_data["stat"]['distance'] / stat_total_distance

        for i in range(0, 4):
            da_sd_stat["overspeeding"][i] = da_sd_stat["overspeeding"][i] * (stat_distance_coef) + trip_data["stat"]['overspeeding'][i] * current_trip_distance_coef

        # Флаг для проверки новый авто или нет
        new_vehicle = True

        # Проверка: новый авто или нет
        for index, stat in enumerate(da_sd_stat["vehicle_stat"]):
            if stat["source_id"] == trip_data["source_id"]:
                # Берем существующие данные по текущему авто
                da_sd_stat["vehicle_stat"][index]["mileage"] = trip_data["stat"]["mileage"]
                da_sd_stat["vehicle_stat"][index]["engine_run_time"] = trip_data["stat"]["engine_run_time"]
                new_vehicle = False
                logger.info(f"Updated statistics for {source_id}")

        if new_vehicle:
            # Создаем новую статистику, так как текущий авто - новый
            new_stat = {
                "source_id": source_id,
                "mileage": trip_data["stat"]['mileage'],
                "engine_run_time": trip_data["stat"]['engine_run_time']
            }

            # Добавляем статистику для нового авто
            da_sd_stat["vehicle_stat"].append(new_stat)
            logger.info(f"Added new statistics for {source_id}")

        # Обновляем score статистики
        da_sd_stat["score"] = total_score

    ids_for_logger = da_sd_stat["user_id"]
    try:
        mongo_hook.insert_one(
            mongo_collection="da_sd_stat",
            doc=da_sd_stat
        )
        logger.info(f"Successfully updated statistics for user id {', '.join(map(str, ids_for_logger))}.")
    except Exception as e:
        logger.info(f"An error occurred during the data update: {e}")


@task(task_id="update_stat")
def update_stat(**kwargs):
    da_sd_trip = kwargs["task_instance"].xcom_pull(task_ids="make_trip")

    postgres_hook = PostgresHook(postgres_conn_id="PostgreSQL")
    mongo_hook = MongoHook(conn_id="MongoDB")

    logger = LoggingMixin().log
    calculate_statistics(da_sd_trip, postgres_hook, mongo_hook, logger)

    return None
