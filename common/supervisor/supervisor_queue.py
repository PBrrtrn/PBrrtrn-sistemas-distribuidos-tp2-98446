import pika
import pika.exceptions
from typing import Dict, List


class SupervisorQueue:
    def __init__(
            self,
            hostname: str,
            bindings: Dict[str, List[str]],
            exchange_type: str = 'direct'):
        # self.name = name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname))
        self.channel = self.connection.channel()

        # self.channel.queue_declare(self.name, exclusive=True)
        result = self.channel.queue_declare('', exclusive=True)
        self.queue_name = result.method.queue
        for exchange, routing_keys in bindings.items():
            self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
            for routing_key in routing_keys:
                self.channel.queue_bind(exchange=exchange,
                                        queue=self.queue_name,
                                        routing_key=routing_key)

    def read(self, timeout: float):
        if timeout < 0:
            return None

        (_method, _properties, body) = next(self.channel.consume(
            queue=self.queue_name,
            inactivity_timeout=timeout,
            auto_ack=True))
        return body

    def close(self):
        self.channel.cancel()
        self.channel.close()
        self.connection.close()
