# ==============================================================================
# Copyright 2024 Sreda Software Solutions. All rights reserved.
# The copyright notice above does not evidence any actual or
# intended publication of such source code. The code contains
# Sreda Software Solutions Confidential Proprietary Information.
# ==============================================================================

"""
Задача для обработки и классификации данных о превышении скорости.

Этот модуль определяет одну задачу для DAG Airflow, которая обрабатывает данные телеметрии для
расчета превышения скорости и классификации его по разным классам превышения скорости.

Атрибуты:
    map_matched_telemetry (pandas.DataFrame): Данные телеметрии, возвращаемые задачей map_match.
    overspeed_class_dict (dict): Словарь, определяющий пороги превышения скорости и их классы.

Функции:
    detect_speedings(**kwargs): определяет нарушения скоростного режима.

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""

from airflow.decorators import task
import numpy as np
import pandas as pd


@task(task_id="detect_speedings")
def detect_speedings(**kwargs) -> pd.DataFrame:
    """
    Python operator, отвечающий за определение нарушения скоростного режима

    Returns:
        map_matched_telemetry: pd.DataFrame
    """

    map_matched_telemetry = kwargs['task_instance'].xcom_pull(task_ids='map_match')

    # Основной источник скорости - obd адаптер,
    # если данных от obd нет, берем скорость из датчиков мобильного устройства - "app".
    map_matched_telemetry['speed_src'] = np.where(
        map_matched_telemetry.speed_obd.isna(),
        "app",
        "obd"
    )

    map_matched_telemetry['overspeed'] = np.where(
        map_matched_telemetry.speed_obd.isna(),
        map_matched_telemetry['speed'] - map_matched_telemetry['speed_limit'],
        map_matched_telemetry['speed_obd'] - map_matched_telemetry['speed_limit']
    )

    # Получившиеся отрицательные значения overspeed меняем на 0
    map_matched_telemetry['overspeed'] =  map_matched_telemetry['overspeed'].clip(lower=0)

    # Сопоставление класса нарушения от значения превышения
    # км/ч    | класс
    # {0}     | 0
    # (0-20]  | 1
    # (20-40] | 2
    # (40+}   | 3
    # https://stackoverflow.com/questions/71045368/mapping-interval-using-map-function-in-python
    overspeed_class_dict = {
        0: 1,
        20: 2,
        40: 3
    }

    map_matched_telemetry['overspeed_class'] = pd.cut(
        map_matched_telemetry['overspeed'],
        right=False,
        bins=[*list(overspeed_class_dict.keys()), np.inf],
        labels=overspeed_class_dict.values()
    ).astype("int")

    map_matched_telemetry.loc[map_matched_telemetry.overspeed == 0, 'overspeed_class'] = 0

    return map_matched_telemetry
