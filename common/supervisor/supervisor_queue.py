import pika
import pika.exceptions
from typing import Dict, List


class SupervisorQueue:
    def __init__(
            self,
            hostname: str,
            name: str,
            bindings: Dict[str, List[str]],
            exchange_type: str = 'direct'):
        self.name = name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=hostname))
        self.channel = self.connection.channel()

        self.channel.queue_declare(self.name, exclusive=True)
        for exchange, routing_keys in bindings.items():
            self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
            for routing_key in routing_keys:
                self.channel.queue_bind(exchange=exchange,
                                        queue=self.name,
                                        routing_key=routing_key)

    def read(self, timeout: float):
        _method, _properties, body = next(self.channel.consume(queue=self.name, inactivity_timeout=timeout))
        return body

    # def read(self, timeout: float):
    #     if timeout < 0:
    #         return None
    #
    #     self.message = None
    #     self.connection.process_data_events(time_limit=timeout)
    #
    #     return self.message

    def close(self):
        self.channel.cancel()
        self.channel.close()
        self.connection.close()