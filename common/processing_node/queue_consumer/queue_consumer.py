from typing import Callable, List

from common.rabbitmq.queue import Queue
import common.network.constants
from common.processing_node.queue_consumer.eof_handler import EOFHandler


class QueueConsumer:
    def __init__(self,
                 process_input: Callable,
                 input_eofs: List[bytes],
                 n_input_peers: int,
                 input_queue: Queue,
                 output_processor,
                 eof_handler: EOFHandler):
        self.process_input = process_input
        self.input_eofs = input_eofs
        self.n_input_peers = n_input_peers
        self.input_queue = input_queue
        self.output_processor = output_processor
        self.eof_handler = eof_handler

    def run(self):
        HEADER_TYPE_LEN = common.network.constants.HEADER_TYPE_LEN
        CLIENT_ID_LEN = common.network.constants.CLIENT_ID_LEN
        if self.eof_handler.number_of_received_eof_signals() == self.n_input_peers:
            #self.__finish_processing_and_close()
            pass #Habdría q preguntar por el último
        else:
            for (channel, method, properties, message) in self.input_queue.read_with_props():
                message_type = message[:HEADER_TYPE_LEN]
                client_id = message[HEADER_TYPE_LEN:HEADER_TYPE_LEN + CLIENT_ID_LEN].decode('utf-8')
                message_body = message[HEADER_TYPE_LEN + CLIENT_ID_LEN:]
                if message_type in self.input_eofs:
                    self.register_eof(channel, method, client_id)
                else:
                    result = self.process_input(message_type, message_body, client_id)
                    self.output_processor.process_output(channel, result, method, properties, client_id)

    def register_eof(self, channel, method, client_id):
        self.eof_handler.two_phase_commit(channel, method)
        # Cuando el processingNode esté andando bien, debe haber un cuidado entre hacer el commit del EOF,
        # hacer el ACK del EOF y enviar el EOF a los siguientes nodos.
        if self.eof_handler.number_of_received_eof_signals() == self.n_input_peers:
            self.__finish_processing_and_close(client_id)

    def __finish_processing_and_close(self, client_id):
        self.output_processor.finish_processing(client_id)
        self.input_queue.close()
