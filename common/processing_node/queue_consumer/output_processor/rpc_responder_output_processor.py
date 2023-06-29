import json

from common.rabbitmq.queue import Queue
from common.processing_node.queue_consumer.output_processor.storage_handler import StorageHandler
from common.processing_node.queue_consumer.forwarding_state_storage_handler import ForwardingStateStorageHandler
from common.rabbitmq.rpc_client import RPCClient

DIR = '.eof'
FILENAME = 'eof_sent_rpc'


class RPCResponderOutputProcessor:
    def __init__(self, rpc_queue: Queue, client_id, optional_rpc_eof: RPCClient = None,
                 optional_rpc_eof_byte: bytes = None):
        self.rpc_queue = rpc_queue
        self.optional_rpc_eof = optional_rpc_eof
        self.optional_rpc_eof_byte = optional_rpc_eof_byte
        self.forwarding_state_storage_handler = ForwardingStateStorageHandler(
            storage_directory=DIR,
            filename=FILENAME,
            client_id=client_id
        )

    def process_output(self, channel, message: bytes, method, properties, _client_id, _message_id):
        # if self.storage["id_last_message_responded"] == message.id: #Message id hay q cargarlo
        #    channel.basic_ack(delivery_tag=method.delivery_tag)

        self.forwarding_state_storage_handler.prepare_last_message_id_increment()
        self.rpc_queue.respond(
            message=message,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
        )
        self.forwarding_state_storage_handler.commit()
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def finish_processing(self, _client_id, _message_id):
        storage = self.forwarding_state_storage_handler.get_storage()
        if not storage.get("rpc_eof_sent", False) and self.optional_rpc_eof is not None:
            self.forwarding_state_storage_handler.prepare_set_rpc_eof_as_sent()
            self.optional_rpc_eof.write_eof(self.optional_rpc_eof_byte)
            self.forwarding_state_storage_handler.commit()
