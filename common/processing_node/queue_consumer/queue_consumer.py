from typing import Callable

from common.rabbitmq.queue import Queue
import common.network.constants
from common.processing_node.queue_consumer.eof_handler import EOFHandler


class QueueConsumer:
    def __init__(self,
                 process_input: Callable,
                 input_eof: bytes,
                 n_input_peers: int,
                 input_queue: Queue,
                 output_processor):
        self.process_input = process_input
        self.input_eof = input_eof
        self.n_input_peers = n_input_peers
        self.input_queue = input_queue
        self.output_processor = output_processor
        self.eof_handler = EOFHandler()

    def run(self):
        if self.eof_handler.received_eof_signals == self.n_input_peers:
            (result, method, properties) = self.eof_handler.get_last_result()
            self.output_processor.finish_processing(result, method, properties)
            self.input_queue.close()
        else:
            for (method, properties, message) in self.input_queue.read_with_props():
                message_type = message[:common.network.constants.HEADER_TYPE_LEN]
                message_body = message[common.network.constants.HEADER_TYPE_LEN:]
                result = self.process_input(message_type, message_body)
                if message_type == self.input_eof:
                    self.register_eof(result, method, properties)
                else:
                    self.output_processor.process_output(result, method, properties)

    def register_eof(self, result, method, properties):
        self.eof_handler.two_phase_commit(result, method, properties)
        # Cuando el processingNode esté andando bien, debe haber un cuidado entre hacer el commit del EOF,
        # hacer el ACK del EOF y enviar el EOF a los siguientes nodos.
        if self.eof_handler.received_eof_signals == self.n_input_peers:
            self.__finish_processing_and_close(result, method, properties)

    def __finish_processing_and_close(self, result, method, properties):
        self.output_processor.finish_processing(result, method, properties)
        self.input_queue.close()
