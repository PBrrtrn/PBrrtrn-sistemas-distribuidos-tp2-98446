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
            bindings: Dict[str, List[str]],
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

        self.channel.basic_consume(
            queue=self.name,
            on_message_callback=self.on_read_callback,
            auto_ack=True
        )

        self.message = None

    def on_read_callback(self, _ch, _method, _props, body):
        self.message = body

    def read(self, timeout: float = None):
        if not timeout:
            timeout = self.timeout

        self.message = None
        self.connection.process_data_events(time_limit=timeout)

        return self.message
