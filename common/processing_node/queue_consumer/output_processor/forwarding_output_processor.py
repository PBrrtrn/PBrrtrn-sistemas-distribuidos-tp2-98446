from common.processing_node.queue_consumer.forwarding_state_storage_handler import ForwardingStateStorageHandler
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.rpc_client import RPCClient

DIR = '.eof'
FILENAME = 'eof_sent'


class ForwardingOutputProcessor:
    def __init__(self, n_output_peers: int, output_exchange_writer: ExchangeWriter, output_eof: bytes,
                 forward_with_routing_key, optional_rpc_eof: RPCClient = None):
        self.n_output_peers = n_output_peers
        self.output_exchange_writer = output_exchange_writer
        self.output_eof = output_eof
        self.optional_rpc_eof = optional_rpc_eof
        self.forward_with_routing_key = forward_with_routing_key
        self.clients_storage_handler_dict = {} #Cargar de un archivo en donde estén los ids de los clientes
        # {"id_cliente": ForwardingStateStorageHandler}

    def process_output(self, channel, message: bytes, method, _properties, client_id):
        if message is None:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # if self.storage["id_last_message_forwarded"] == message.id: # Message id hay q cargarlo
        #    channel.basic_ack(delivery_tag=method.delivery_tag)
        if client_id not in self.clients_storage_handler_dict:
            self.clients_storage_handler_dict[client_id] = \
                ForwardingStateStorageHandler(storage_directory=DIR, filename=FILENAME, client_id=client_id)
        self.clients_storage_handler_dict[client_id].prepare_last_message_id_increment()
        self._forward(self.output_exchange_writer, message, client_id)
        self.clients_storage_handler_dict[client_id].commit()
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def finish_processing(self, client_id):
        storage = self.clients_storage_handler_dict[client_id].get_storage()
        if not storage.get("rpc_eof_sent", False) and self.optional_rpc_eof is not None:
            self.clients_storage_handler_dict[client_id].prepare_set_rpc_eof_as_sent()
            self.optional_rpc_eof.write_eof(self.output_eof, routing_key_suffix=client_id)
            self.clients_storage_handler_dict[client_id].commit()
        remaining_eofs = self.n_output_peers - storage.get("eofs_sent", 0)
        for i in range(remaining_eofs):
            self.clients_storage_handler_dict[client_id].prepare_eofs_sent_increment()
            self._forward_eof(self.output_exchange_writer, self.output_eof, client_id)
            # self.output_exchange_writer.write(self.output_eof, routing_key_suffix='1')
            self.clients_storage_handler_dict[client_id].commit()

    def _forward(self, exchange_writer, message, client_id):
        if self.forward_with_routing_key:
            exchange_writer.write(message, routing_key_suffix=client_id)
        else:
            exchange_writer.write(message)

    def _forward_eof(self, exchange_writer, output_eof, client_id):
        if self.forward_with_routing_key:
            exchange_writer.write(output_eof + client_id.encode(), routing_key_suffix=client_id)
        else:
            exchange_writer.write(output_eof + client_id.encode())
