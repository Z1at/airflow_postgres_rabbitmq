# ==============================================================================
# Copyright 2024 Sreda Software Solutions. All rights reserved.
# The copyright notice above does not evidence any actual or
# intended publication of such source code. The code contains
# Sreda Software Solutions Confidential Proprietary Information.
# ==============================================================================

"""
Задача для определения маневров поездки.

Этот модуль определяет одну задачу для DAG Airflow, которая использует
метод trace_route из библиотеки Valhalla для построения маршрута,
которому присущи инструкции для движения. Эти инструкции содержат информацию
о различных типах маневров https://valhalla.github.io/valhalla/api/turn-by-turn/api-reference/.

Атрибуты:
    map_matched_telemetry (pandas.DataFrame): Привязанная к дорожному графу телеметрия, полученная от задачи map_match.
    trip_locations (pandas.DataFrame): Набор данных с точками телеметрии по основному маршруту.
    trip_maneuvers (pandas.DataFrame): Набор данных с точками маневров по основному маршруту.
    maneuvers_df (pandas.DataFrame): Объединение trip_locations и trip_maneuvers по индексу.
    alternate_maneuvers_df (pandas.DataFrame): Аналогично maneuvers_df в случае возврата Valhalla поля alternates.
    maneuvers_map_dict (dict): Словарь, определяющий типы маневров.
    https://valhalla.github.io/valhalla/api/turn-by-turn/api-reference/#:~:text=For%20the%20maneuver%20type%2C%20the%20following%20are%20available%3A
    maneuver_documents (list): Список определенных маневров во время поездки.

Функции:
    process_trace_route_data(dict, dict): обрабатывает данные, возвращаемые запросом \trace_route.
    get_maneuvers(**kwargs): определяет маневры во время поездки с помощью библиотеки Valhalla.

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""

import json
import requests

import pandas as pd
import numpy as np
from airflow.decorators import task

VALHALLA_URL = 'http://sd-valhalla.default:8002'


def process_trace_route_data(locations: dict, legs: dict):
    """
    Обработка полученных данных из запроса /trace_route

    Returns:
        pandas.DataFrame: _description_
    """
    trip_locations = pd.DataFrame(locations)
    trip_maneuvers = pd.DataFrame([maneuver['maneuvers'][1] for maneuver in legs])

    # trip_maneuvers таблица, каждая строка которой отвечает за инструкцию между точками trip_locations
    maneuvers = pd.merge(trip_locations, trip_maneuvers, right_index=True, left_index=True)

    # Если прямая инструкция "Вы прибыли в пункт назначения.",
    # то для прохождения в следующую точку маршрута не нужно совершать маневр.
    maneuvers = maneuvers[maneuvers.instruction != "Вы прибыли в пункт назначения."]

    return maneuvers


@task(task_id="get_maneuvers")
def get_maneuvers(**kwargs) -> list[dict]:
    """
    Python operator, обеспечивающий определение маневров.

    Returns:
        maneuver_documents list[dict]: Список маневров формата feature geojson.
    """

    map_matched_telemetry = kwargs['task_instance'].xcom_pull(task_ids='map_match')

    # Убираем дубликаты по [направлению, широте, долготе] для упрощения запроса к Valhalla
    map_matched_telemetry_without_dups = map_matched_telemetry.drop_duplicates(
        subset=['heading', 'longitude', 'latitude']
        ).reset_index()

    # request - тело запроса с параметрами для метода Valhalla trace_route
    request = {
        "shape": [
            {
                "lat": x["latitude"],
                "lon": x["longitude"],
                "type": "break_through",
                "way_id": x['way_id']
            } for x in map_matched_telemetry_without_dups.to_dict('records')
        ],
        "costing": "auto",
        "shape_match": "map_snap",
        "language": "ru-RU"
    }
    response = json.loads(requests.get(f'{VALHALLA_URL}/trace_route', json=request).content)

    maneuvers_df = process_trace_route_data(response['trip']['locations'], response['trip']['legs'])

    # По непонятной причине Valhalla может разделить основной маршрут на части и выдать их в поле alternates
    # Поэтому добавлена обработка каждой такой части и их конкатенация к основному маршруту
    for alternate_trip in response.get('alternates', []):
        alternate_maneuvers_df = process_trace_route_data(
            alternate_trip['trip']['locations'],
            alternate_trip['trip']['legs']
        )
        maneuvers_df = pd.concat([maneuvers_df, alternate_maneuvers_df])

    maneuvers_map_dict = {
        9: "slight_right_turn",
        10: "right_turn",
        11: "sharp_right_turn",
        12: "u_turn_right",
        13: "u_turn_left",
        14: "slight_right_turn",
        15: "left_turn",
        16: "slight_left_turn",
        26: "roundabout_enter",
        27: "roundabout_exit"
    }
    maneuvers_df = maneuvers_df[maneuvers_df["type_y"].isin(maneuvers_map_dict.keys())]
    maneuvers_df['type_y'].replace(maneuvers_map_dict, inplace=True)

    # Соединение maneuvers_df и map_matched_telemetry_without_dups для получения timestamp и date для каждого маневра
    maneuvers_df = pd.merge(
        maneuvers_df,
        map_matched_telemetry_without_dups[['timestamp', 'date']],
        left_on="original_index",
        right_index=True
    )

    # Формирование выходного списка маневров
    maneuver_documents = []
    for _, maneuver in maneuvers_df.iterrows():
        # Выбираем точки телематики в интервале +-5 секунд
        near_maneuver_df = map_matched_telemetry.loc[
            (map_matched_telemetry.timestamp >= maneuver['timestamp'] - 5000) &
            (map_matched_telemetry.timestamp <= maneuver['timestamp'] + 5000)
        ].copy()
        # Вычисление длины вектора ускорения по 3 осям
        near_maneuver_df['acceleration'] = np.linalg.norm(
            near_maneuver_df.loc[:,['acceleration_x', 'acceleration_y', 'acceleration_z']].values,
            axis=1
        )

        maneuver_document = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [maneuver['lon'], maneuver['lat']]
            },
            "properties": {
                "timestamp": maneuver['timestamp'],
                "date": maneuver['date'].to_pydatetime(),
                "event_type": maneuver['type_y'],
                "duration": maneuver['time'],
                "from_way": int(near_maneuver_df.iloc[0]['way_id']),
                "to_way": int(near_maneuver_df.iloc[-1]['way_id']),
                "angle": abs(near_maneuver_df.iloc[0]['heading'] - near_maneuver_df.iloc[-1]['heading']),
                "speed": {
                    "from": near_maneuver_df['speed'].iloc[0],
                    "to": near_maneuver_df['speed'].iloc[-1],
                    "max": near_maneuver_df['speed'].max(),
                    "min": near_maneuver_df['speed'].min(),
                    "mean": near_maneuver_df['speed'].mean(),
                    "q50": near_maneuver_df['speed'].quantile(0.5),
                    "q25": near_maneuver_df['speed'].quantile(0.25),
                    "q75": near_maneuver_df['speed'].quantile(0.75)
                },
                "dangerous": bool(near_maneuver_df['acceleration'].quantile(0.5) > 2.5)
            }
        }
        maneuver_documents.append(maneuver_document)

    return maneuver_documents
