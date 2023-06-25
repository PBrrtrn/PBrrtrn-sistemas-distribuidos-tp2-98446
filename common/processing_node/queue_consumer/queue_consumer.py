from typing import Callable

from common.rabbitmq.queue import Queue
import common.network.constants


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

        self.received_eof_signals = 0

    def run(self):
        for (method, properties, message) in self.input_queue.read_with_props():
            message_type = message[:common.network.constants.HEADER_TYPE_LEN]
            message_body = message[common.network.constants.HEADER_TYPE_LEN:]
            result = self.process_input(message_type, message_body)
            if message_type == self.input_eof:
                self.register_eof(result, method, properties)
            else:
                self.output_processor.process_output(result, method, properties)

    def register_eof(self, result, method, properties):
        self.received_eof_signals += 1
        # Cuando el processingNode esté andando bien, debe haber un cuidado entre hacer el commit del EOF,
        # hacer el ACK del EOF y enviar el EOF a los siguientes nodos.
        if self.received_eof_signals == self.n_input_peers:
            self.output_processor.finish_processing(result, method, properties)
            self.input_queue.close()
