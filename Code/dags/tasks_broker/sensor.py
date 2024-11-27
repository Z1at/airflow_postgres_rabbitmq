from rabbitmq_provider.sensors.rabbitmq import RabbitMQSensor


sensor = RabbitMQSensor(
    task_id="sensor",
    queue_name="queue.airflow",
    rabbitmq_conn_id="RabbitMQ",
)
