from common.processing_node.queue_consumer.forwarding_state_storage_handler import ForwardingStateStorageHandler
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.rpc_client import RPCClient

DIR = '.eof'
FILENAME = 'eof_sent'
COMMIT_CHAR = "C\n"


class ForwardingOutputProcessor:
    def __init__(self, n_output_peers: int, output_exchange_writer: ExchangeWriter, output_eof: bytes,
                 forward_with_routing_key, optional_rpc_eof: RPCClient = None):
        self.n_output_peers = n_output_peers
        self.output_exchange_writer = output_exchange_writer
        self.output_eof = output_eof
        self.optional_rpc_eof = optional_rpc_eof
        self.forward_with_routing_key = forward_with_routing_key
        self.forwarding_state_storage_handler = ForwardingStateStorageHandler(
            storage_directory=DIR,
            filename=FILENAME
        )

    def process_output(self, channel, message: bytes, method, _properties):
        if message is None:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # if self.storage["id_last_message_forwarded"] == message.id: # Message id hay q cargarlo
        #    channel.basic_ack(delivery_tag=method.delivery_tag)

        self.forwarding_state_storage_handler.prepare_last_message_id_increment()
        self._forward(self.output_exchange_writer, message)
        self.forwarding_state_storage_handler.commit()
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def finish_processing(self):
        storage = self.forwarding_state_storage_handler.get_storage()
        if not storage.get("rpc_eof_sent", False) and self.optional_rpc_eof is not None:
            self.forwarding_state_storage_handler.prepare_set_rpc_eof_as_sent()
            self.optional_rpc_eof.write_eof(self.output_eof, routing_key_suffix='1')
            self.forwarding_state_storage_handler.commit()
        remaining_eofs = self.n_output_peers - storage.get("eofs_sent", 0)
        for i in range(remaining_eofs):
            self.forwarding_state_storage_handler.prepare_eofs_sent_increment()
            self._forward(self.output_exchange_writer, self.output_eof)
            # self.output_exchange_writer.write(self.output_eof, routing_key_suffix='1')
            self.forwarding_state_storage_handler.commit()

    def _forward(self, exchange_writer, message):
        if self.forward_with_routing_key:
            exchange_writer.write(message, routing_key_suffix='1')
        else:
            exchange_writer.write(message)
