from typing import Callable

from common.rabbitmq.queue_reader import QueueReader
import common.network.constants


class RPCServer:
    def __init__(self, queue_reader: QueueReader, processing_callback):
        self.queue_reader = queue_reader
        self.processing_callback = processing_callback

    def serve(self):
        self.queue_reader.consume(self.rpc_callback)

    def rpc_callback(self, _channel, method, properties, body):
        request_type = body[:common.network.constants.HEADER_TYPE_LEN]
        request_body = body[common.network.constants.HEADER_TYPE_LEN:]
        result = self.processing_callback(request_type, request_body)

        self.queue_reader.respond(
            message=result,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
            delivery_tag=method.delivery_tag
        )

