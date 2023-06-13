from common.rabbitmq.queue import Queue
import common.network.constants
from output_handler import OutputHandler


class ProcessingNode:
    def __init__(self, processor, n_input_peers: int, input_queue: Queue, output_handler: OutputHandler):
        self.processor = processor
        self.n_input_peers = n_input_peers
        self.input_queue = input_queue
        self.eof_registry = {}
        self.output_handler = output_handler
        self.running = False

    def run(self):
        self.running = True
        while self.running:
            message = self.input_queue.read()
            message_type = message[:common.network.constants.HEADER_TYPE_LEN]
            message_body = message[common.network.constants.HEADER_TYPE_LEN:]

            if message_type == common.network.constants.EOF:
                self.register_eof(message_body)
            else:
                result = self.processor.process(message_type, message_body)
                self.output_handler.output_message(result)

    def register_eof(self, client_id):
        new_value = self.eof_registry.get(client_id, 0) + 1
        self.eof_registry[client_id] = new_value

        if self.eof_registry[client_id] == self.n_input_peers:
            self.output_handler.output_eof(client_id)
