from common.rabbitmq.queue import Queue
from common.processing_node.storage_handler import StorageHandler


class RPCResponderOutputProcessor:
    def __init__(self, rpc_queue: Queue, storage_handler: StorageHandler):
        self.rpc_queue = rpc_queue
        self.storage_handler = storage_handler

    def process_output(self, message: bytes, method, properties):
        self.rpc_queue.respond(
            message=message,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
            delivery_tag=method.delivery_tag
        )

    def finish_processing(self, message: bytes, method, properties):
        # Eventualmente tmb recibe el id del cliente
        self.storage_handler.prepare_delete()
        self.rpc_queue.respond(
            message=message,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
            delivery_tag=method.delivery_tag
        )
        self.storage_handler.commit_delete()
