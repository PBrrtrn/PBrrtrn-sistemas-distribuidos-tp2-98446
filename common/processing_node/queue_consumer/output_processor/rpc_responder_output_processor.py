from common.rabbitmq.queue import Queue
from common.processing_node.storage_handler import StorageHandler


class RPCResponderOutputProcessor:
    def __init__(self, rpc_queue: Queue, storage_handler: StorageHandler):
        self.rpc_queue = rpc_queue
        self.storage_handler = storage_handler

    def process_output(self, channel, message: bytes, method, properties):
        self.rpc_queue.respond(
            message=message,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
            delivery_tag=method.delivery_tag
        )
        #Commit de que se escribió el mensaje
        #ACK
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def finish_processing(self, message: bytes, delivery_tag, correlation_id, reply_to):
        # Eventualmente tmb recibe el id del cliente
        self.storage_handler.prepare_delete()
        self.rpc_queue.respond(
            message=message,
            to=reply_to,
            correlation_id=correlation_id,
            delivery_tag=delivery_tag
        )
        self.storage_handler.commit_delete()
