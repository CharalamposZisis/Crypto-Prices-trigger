import pika
import json

RABBITMQ_HOST = "localhost"
RABBITMQ_USER = "user"
RABBITMQ_PASS = "password"
RABBITMQ_QUEUE = "alerts"

_rmq_connection = None
_rmq_channel = None

def init_rabbitmq():
    global _rmq_connection, _rmq_channel

    if _rmq_connection and _rmq_connection.is_open:
        print("[INFO] RabbitMQ connection already initialized")
        return

    _rmq_connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
            heartbeat=30
        )
    )
    _rmq_channel = _rmq_connection.channel()
    _rmq_channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)


def publish_to_rabbitmq(event):
    try:
        if not _rmq_connection or _rmq_connection.is_closed:
            init_rabbitmq()
        _rmq_channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(event),
            properties=pika.BasicProperties(delivery_mode=2)
        )
    except Exception as e:
        print("[WARN] RabbitMQ publish failed, reconnecting:", repr(e))
        try:
            init_rabbitmq()
        except Exception:
            pass