from time import sleep

import pika
import pika.exceptions
from typing import Dict, List

DEFAULT_TIMEOUT = None


class Queue:
    MAX_RETRIES = 10

    def __init__(
            self,
            hostname: str,
            name: str,
            bindings: Dict[str, List[str]] = {},
            exchange_type: str = 'direct',
            timeout: int = DEFAULT_TIMEOUT):
        self.name = name
        self.timeout = timeout

        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname))
                break
            except pika.exceptions.AMQPConnectionError:
                retries += 1
                sleep(0.5)

        if retries == self.MAX_RETRIES:
            print(f"ERROR - Failed to connect to RabbitMQ")
            raise Exception("Failed to connect to RabbitMQ")
        self.channel = self.connection.channel()

        self.channel.queue_declare(self.name)
        for exchange, routing_keys in bindings.items():
            self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
            for routing_key in routing_keys:
                self.channel.queue_bind(exchange=exchange,
                                        queue=self.name,
                                        routing_key=routing_key)

    def read(self):
        for (_method, _properties, body) in self.channel.consume(queue=self.name, inactivity_timeout=self.timeout):
            yield body

    def read_with_props(self):
        for (method, properties, body) in self.channel.consume(queue=self.name,
                                                                 inactivity_timeout=self.timeout):
            yield (method, properties, body)

    def respond(self, message: bytes, to: str, correlation_id, delivery_tag):
        self.channel.basic_publish(exchange='',
                                   routing_key=to,
                                   properties=pika.BasicProperties(correlation_id=correlation_id),
                                   body=message)

        self.channel.basic_ack(delivery_tag=delivery_tag)

    def close(self):
        self.channel.cancel()
        self.channel.close()
        self.connection.close()
