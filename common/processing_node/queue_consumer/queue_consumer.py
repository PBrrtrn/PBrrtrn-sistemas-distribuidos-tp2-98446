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
        if self.eof_handler.number_of_received_eof_signals() == self.n_input_peers:
            (last_result, last_delivery_tag, last_correlation_id, last_reply_to) =\
                self.eof_handler.get_last_result()
            self.__finish_processing_and_close(last_result, last_delivery_tag,
                                               last_correlation_id, last_reply_to)
        else:
            for (channel, method, properties, message) in self.input_queue.read_with_props():
                message_type = message[:common.network.constants.HEADER_TYPE_LEN]
                message_body = message[common.network.constants.HEADER_TYPE_LEN:]
                result = self.process_input(message_type, message_body)
                if message_type in self.input_eofs:
                    self.register_eof(channel, result, method, properties)
                else:
                    self.output_processor.process_output(channel, result, method, properties)

    def register_eof(self, channel, result, method, properties):
        self.eof_handler.two_phase_commit(channel, result, method, properties)
        # Cuando el processingNode esté andando bien, debe haber un cuidado entre hacer el commit del EOF,
        # hacer el ACK del EOF y enviar el EOF a los siguientes nodos.
        if self.eof_handler.number_of_received_eof_signals() == self.n_input_peers:
            self.__finish_processing_and_close(result, method.delivery_tag,
                                               properties.correlation_id, properties.reply_to)

    def __finish_processing_and_close(self, result, delivery_tag, correlation_id, reply_to):
        self.output_processor.finish_processing(result, delivery_tag, correlation_id, reply_to)
        self.input_queue.close()
