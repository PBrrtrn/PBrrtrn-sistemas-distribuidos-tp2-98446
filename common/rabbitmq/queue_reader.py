from time import sleep

import pika
import pika.exceptions
from typing import Callable, Dict, List


class QueueReader:
    MAX_RETRIES = 10

    def __init__(self, queue_name: str, queue_bindings: Dict[str, List[str]] = {}, exchange_type: str = 'direct'):
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
                break
            except pika.exceptions.AMQPConnectionError:
                retries += 1
                sleep(0.5)

        if retries == self.MAX_RETRIES:
            print(f"ERROR - Failed to connect to RabbitMQ")
            raise Exception("Failed to connect to RabbitMQ")

        self.channel = self.connection.channel()
        self.queue_name = queue_name

        self.channel.queue_declare(queue=self.queue_name)
        for exchange, routing_keys in queue_bindings.items():
            self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
            for routing_key in routing_keys:
                self.channel.queue_bind(exchange=exchange,
                                        queue=queue_name,
                                        routing_key=routing_key)

        self.consumer_tag = None

    def consume(self, callback: Callable, auto_ack=True):
        self.consumer_tag = self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=auto_ack
        )

        self.channel.start_consuming()

    def respond(self, message: bytes, to: str, correlation_id, delivery_tag):
        self.channel.basic_publish(exchange='',
                                   routing_key=to,
                                   properties=pika.BasicProperties(correlation_id=correlation_id),
                                   body=message)

        self.channel.basic_ack(delivery_tag=delivery_tag)

    def shutdown(self):
        # self.channel.basic_cancel(self.consumer_tag)
        self.connection.close()
