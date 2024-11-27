from rabbitmq_provider.sensors.rabbitmq import RabbitMQSensor


extract = RabbitMQSensor(
    task_id="extract",
    queue_name="queue.airflow",
    rabbitmq_conn_id="RabbitMQ",
)
