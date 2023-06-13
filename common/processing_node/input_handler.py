from common.rabbitmq.queue import Queue
import common.network.constants


class InputHandler:
    def __init__(self, n_input_peers: int, input_queue: Queue):
        self.n_input_peers = n_input_peers
        self.input_queue = input_queue

        self.eof_registry = {}

    def get_input(self):
        message = self.input_queue.read()
        message_type = message[:common.network.constants.HEADER_TYPE_LEN]
        message_body = message[common.network.constants.HEADER_TYPE_LEN:]

        if message_type == common.network.constants.EOF:
            self.register_eof(message_body)

        return message_type, message_body

    def register_eof(self, client_id):
        new_value = self.eof_registry.get(client_id, 0) + 1
        self.eof_registry[client_id] = new_value
