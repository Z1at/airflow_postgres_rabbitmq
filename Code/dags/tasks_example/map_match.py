# ==============================================================================
# Copyright 2024 Sreda Software Solutions. All rights reserved.
# The copyright notice above does not evidence any actual or
# intended publication of such source code. The code contains
# Sreda Software Solutions Confidential Proprietary Information.
# ==============================================================================

"""
Задача для обеспечения привязки данных телеметрии к дорожному графу.

Этот модуль определяет одну задачу для DAG Airflow, которая использует
алгоритм map_matching из библиотеки Valhalla для привязки телеметрии к дорожному графу.

Атрибуты:
    telemetry (pandas.DataFrame): Набор данных телеметрии, возвращаемый задачей export_telemetry.
    matched_points_df (pandas.DataFrame): Набор данных телеметрии [lon, lat, edge_index],
    привязанной к ребрам дорожного графа по индексу.
    edges_df (pandas.DataFrame): Набор данных с информацией о ребрах дорожного графа,
    содержащий данные [way_id, speed, sign, speed_limit].
    telemetry_with_edges (pandas.DataFrame): Набор данных с привязанными точками.

Функции:
    map_match(**kwargs): соединяет исходные данные телеметрии к дорожному графу.

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""

import json
import requests
import pandas as pd
from airflow.decorators import task

VALHALLA_URL = 'http://sd-valhalla.default:8002'


@task(task_id="map_match")
def map_match(**kwargs) -> pd.DataFrame:
    """
    Python operator, обеспечивающий привязку телеметрии к дорожному графу.
    Использует map_matching алгоритм Valhalla (trace_attributes)
    https://valhalla.github.io/valhalla/api/map-matching/api-reference/

    Returns:
        telemetry_with_edges: pandas.DataFrame со следующими атрибутами:
        [timestamp, date, heading, speed, speed_obd, altitude, \
            acceleration_x, acceleration_y, acceleration_z, \
                way_id, speed_limit, longitude, latitude]
    """

    telemetry = kwargs['task_instance'].xcom_pull(task_ids='export_telemetry')
    shape_telemetry = telemetry.rename(columns={"latitude": "lat", "longitude": "lon"})

    # request - тело запроса с параметрами для метода Valhalla trace_attributes
    request = {
        "shape": shape_telemetry[['lat', 'lon', 'heading']].to_dict('records'),
        "costing": "auto",
        "shape_match": "map_snap",
        "trace_options": {
            "search_radius": 10
        },
        "filters": {
            "attributes": [
                "edge.way_id",
                "edge.speed",
                "edge.speed_limit",
                "matched.edge_index",
                "matched.point"
            ],
            "action": "include"
        }
    }
    print(request)

    url_trace_attributes = f'{VALHALLA_URL}/trace_attributes'
    response = requests.get(url_trace_attributes, json=request).content
    trace_route_attributes = json.loads(response)

    matched_points_df: pd.Series = pd.DataFrame(trace_route_attributes['matched_points'])
    edges_df: pd.DataFrame = pd.DataFrame(trace_route_attributes['edges'])

    # Если OSM не содержит информацию об ограничении скоростного режима (speed_limit),
    # информация должна быть заменена на информацию из конфигурации valhalla default_speeds.json
    # https://valhalla.github.io/valhalla/speeds/
    # https://github.com/OpenStreetMapSpeeds/schema

    if 'speed_limit' in edges_df.columns:
        edges_df['speed_limit'].fillna(edges_df['speed'], inplace=True)
    else:
        edges_df['speed_limit'] = edges_df['speed']

    edges_df = edges_df[['way_id', 'speed_limit']]

    # Соединение matched_points_df и edges_df для привязки точкам телеметрии полей:
    # way_id - id привязанного ребра OSM,
    # speed_limit - ограничение скоростного режима для привязанного ребра.

    # Оставляет только привязанные или интерполированные точки lat, lon
    matched_points_with_edges = pd.merge(
        matched_points_df,
        edges_df,
        left_on='edge_index',
        right_index=True
    )[['way_id', 'speed_limit', 'lon', 'lat']]

    # Соединяет исходную телеметрию и привязанные точки
    # Заменяет исходные координаты точек на привязанные/интерполированные
    telemetry_with_edges = pd.merge(telemetry, matched_points_with_edges, left_index=True, right_index=True) \
        .drop(columns=['longitude', 'latitude'])\
        .rename(columns={"lon": "longitude", "lat": "latitude"})\
        .reset_index(drop=True)
    return telemetry_with_edges
