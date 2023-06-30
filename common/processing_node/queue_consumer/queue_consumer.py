from typing import Callable, List, Dict

from common.rabbitmq.queue import Queue
import common.network.constants
from common.processing_node.queue_consumer.eof_handler import EOFHandler


def get_fields_from_message(message):
    header_type_len = common.network.constants.HEADER_TYPE_LEN
    client_id_len = common.network.constants.CLIENT_ID_LEN
    message_id_len = common.network.constants.MESSAGE_ID_LEN
    message_type = message[:header_type_len]
    client_id = message[header_type_len:header_type_len + client_id_len].decode()
    message_id = message[header_type_len + client_id_len:
                         header_type_len + client_id_len + message_id_len].decode()
    message_body = message[header_type_len + client_id_len + message_id_len:]
    return message_type, client_id, message_id, message_body


class QueueConsumer:
    def __init__(self,
                 process_input: Callable,
                 input_eofs: List[bytes],
                 n_input_peers: int,
                 input_queue: Queue,
                 output_processor,
                 eof_handlers_dict: Dict[str, EOFHandler],
                 many_clients: bool = True):
        self.process_input = process_input
        self.input_eofs = input_eofs
        self.n_input_peers = n_input_peers
        self.input_queue = input_queue
        self.output_processor = output_processor
        self.eof_handlers_dict = eof_handlers_dict
        self.many_clients = many_clients

    def run(self):
        for client_id in self.eof_handlers_dict:
            if self.eof_handlers_dict[client_id].number_of_received_eof_signals() == self.n_input_peers:
                message_id = self.eof_handlers_dict[client_id].get_last_message_id()
                self.__finish_processing_and_close(client_id, message_id)
        for (channel, method, properties, message) in self.input_queue.read_with_props():
            message_type, client_id, message_id, message_body = get_fields_from_message(message)
            if message_type in self.input_eofs:
                self.register_eof(channel, method, client_id, message_id)
            else:
                result = self.process_input(message_type, message_body, client_id, message_id)
                self.output_processor.process_output(channel, result, method, properties, client_id, message_id)

    def register_eof(self, channel, method, client_id, message_id):
        if client_id not in self.eof_handlers_dict:
            self.eof_handlers_dict[client_id] = EOFHandler(".eof", filename="eof_received", client_id=client_id)
        self.eof_handlers_dict[client_id].register_eof(channel, method, message_id)
        if self.eof_handlers_dict[client_id].number_of_received_eof_signals() == self.n_input_peers:
            self.__finish_processing_and_close(client_id, message_id)

    def __finish_processing_and_close(self, client_id, message_id):
        self.output_processor.finish_processing(client_id, message_id)
        if not self.many_clients:
            print(f"Finishing work for client {client_id}.")
            self.input_queue.close()
