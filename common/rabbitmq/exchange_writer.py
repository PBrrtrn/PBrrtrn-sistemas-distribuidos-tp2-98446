from time import sleep

import pika
import pika.exceptions


class ExchangeWriter:
    MAX_RETRIES = 10

    def __init__(self, hostname: str, exchange_name: str, exchange_type: str = 'direct', queue_name: str = ''):
        retries = 0
        while retries < self.MAX_RETRIES:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname))
                break
            except pika.exceptions.AMQPConnectionError:
                retries += 1
                sleep(3)

        if retries == self.MAX_RETRIES:
            print(f"ERROR - Failed to connect to RabbitMQ")
            raise Exception("Failed to connect to RabbitMQ")

        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type=exchange_type)

    def write(self, message: bytes, routing_key: str = None):
        if not routing_key:
            routing_key = self.queue_name

        self.channel.basic_publish(exchange=self.exchange_name,
                                   routing_key=routing_key,
                                   body=message,
                                   properties=pika.BasicProperties(
                                       delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                                   ))

    def shutdown(self):
        self.channel.close()
