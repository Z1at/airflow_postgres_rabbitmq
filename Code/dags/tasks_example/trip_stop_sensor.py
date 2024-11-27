# ==============================================================================
# Copyright 2024 Sreda Software Solutions. All rights reserved.
# The copyright notice above does not evidence any actual or
# intended publication of such source code. The code contains
# Sreda Software Solutions Confidential Proprietary Information.
# ==============================================================================

"""
Задача для считывания сообщения из очереди.

Этот модуль определяет одну задачу для DAG Airflow,
в которой сенсор реагирует на сообщение в очереди "queue.internal.airflow.trip_stop_event".

Атрибуты:
    trip_stop_sensor: сенсор RabbitMQ, реагирующий на каждое сообщение, пришедшее в очередь.
    В данном случае сенсор реагирует на сообщение в очереди "queue.internal.airflow.trip_stop_event"
    Сообщение имеет следующую структуру:
    {
        "source_id": "1", (str)
        "trip_stop_date": "2020-06-26T12:29:01.000" //ISODate str
    }

Использование:
    Этот модуль предназначен для использования в качестве задачи в DAG Airflow.
"""

from rabbitmq_provider.sensors.rabbitmq import RabbitMQSensor


trip_stop_sensor = RabbitMQSensor(
    task_id="trip_stop_sensor",
    queue_name="queue.internal.airflow.trip_stop_event",
    rabbitmq_conn_id="RabbitMQ",
)
