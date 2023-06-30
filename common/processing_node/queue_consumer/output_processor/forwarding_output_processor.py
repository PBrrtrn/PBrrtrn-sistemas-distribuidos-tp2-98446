from common.processing_node.queue_consumer.forwarding_state_storage_handler import ForwardingStateStorageHandler
from common.processing_node.queue_consumer.client_list_storage_handler import ClientListStorageHandler
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.rpc_client import RPCClient

DIR = '.eof'
CLIENT_LOG_FILENAME = 'eof_sent'
CLIENTS_LIST_FILENAME = 'clients_list'

class ForwardingOutputProcessor:
    def __init__(self, n_output_peers: int, output_exchange_writer: ExchangeWriter, output_eof: bytes,
                 forward_with_routing_key: bool, current_client_list, optional_rpc_eof: RPCClient = None):
        self.n_output_peers = n_output_peers
        self.output_exchange_writer = output_exchange_writer
        self.output_eof = output_eof
        self.optional_rpc_eof = optional_rpc_eof
        self.forward_with_routing_key = forward_with_routing_key
        self.clients_storage_handler_dict = {}
        for client_id in current_client_list:
            self.clients_storage_handler_dict[client_id] = \
                ForwardingStateStorageHandler(storage_directory=DIR, filename=CLIENT_LOG_FILENAME, client_id=client_id)

    def process_output(self, channel, message: bytes, method, _properties, client_id, message_id):
        if message is None:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return
        if client_id not in self.clients_storage_handler_dict:
            self._create_clients_storage_handler_for_client(client_id)
        client_storage_handler = self.clients_storage_handler_dict[client_id]
        if client_storage_handler.get_storage().get("id_last_message_forwarded", 0) == message_id:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return
        client_storage_handler.prepare_last_message_id_increment(message_id)
        self._forward(self.output_exchange_writer, message, client_id)
        client_storage_handler.commit()
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def finish_processing(self, client_id, message_id):
        if client_id not in self.clients_storage_handler_dict:
            self._create_clients_storage_handler_for_client(client_id)
        client_storage_handler = self.clients_storage_handler_dict[client_id]
        storage = client_storage_handler.get_storage()
        if not storage.get("rpc_eof_sent", False) and self.optional_rpc_eof is not None:
            client_storage_handler.prepare_set_rpc_eof_as_sent()
            self.optional_rpc_eof.write_eof(self.output_eof + client_id.encode() + message_id.encode(),
                                            routing_key_suffix=client_id)
            client_storage_handler.commit()
        remaining_eofs = self.n_output_peers - storage.get("eofs_sent", 0)
        for i in range(remaining_eofs):
            client_storage_handler.prepare_eofs_sent_increment()
            self._forward_eof(self.output_exchange_writer, self.output_eof, client_id, message_id)
            client_storage_handler.commit()

    def _forward(self, exchange_writer, message, client_id):
        if self.forward_with_routing_key:
            exchange_writer.write(message, routing_key_suffix=client_id)
        else:
            exchange_writer.write(message)

    def _forward_eof(self, exchange_writer, output_eof, client_id, message_id):
        if self.forward_with_routing_key:
            exchange_writer.write(output_eof + client_id.encode() + message_id.encode(), routing_key_suffix=client_id)
        else:
            exchange_writer.write(output_eof + client_id.encode() + message_id.encode())

    def _create_clients_storage_handler_for_client(self, client_id):
        self.clients_storage_handler_dict[client_id] = \
            ForwardingStateStorageHandler(storage_directory=DIR, filename=CLIENT_LOG_FILENAME, client_id=client_id)

