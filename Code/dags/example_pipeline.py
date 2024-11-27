# ==============================================================================
# Copyright 2024 Sreda Software Solutions. All rights reserved.
# The copyright notice above does not evidence any actual or
# intended publication of such source code. The code contains
# Sreda Software Solutions Confidential Proprietary Information.
# ==============================================================================

from airflow import DAG
from datetime import datetime
from tasks_example.trip_stop_sensor import trip_stop_sensor
from tasks_example.get_dates_for_trip import get_dates_for_trip
# from tasks_example.export_telemetry import export_telemetry
# from tasks_example.map_match import map_match
# from tasks_example.get_maneuvers import get_maneuvers
# from tasks_example.detect_speedings import detect_speedings
# from tasks_example.make_trip import make_trip
# from tasks_example.update_stat import update_stat


with DAG(
        dag_id="analytics_pipeline",
        schedule_interval=None,
        start_date=datetime.now(),
        schedule="@continuous",
        max_active_runs=1
    ) as dag:
    trip_stop_sensor >> get_dates_for_trip() #>> export_telemetry() >> map_match() # >> [get_maneuvers(), detect_speedings()] >> make_trip() >> update_stat()
    