from typing import Callable

from common.rabbitmq.queue import Queue
import common.network.constants
from common.supervisor.supervisor_process import SupervisorProcess


class ProcessingNode:
    def __init__(self,
                 process_input: Callable,
                 input_eof: bytes,
                 n_input_peers: int,
                 input_queue: Queue,
                 output_processor,
                 supervisor_process: SupervisorProcess = None):
        self.process_input = process_input
        self.input_eof = input_eof
        self.n_input_peers = n_input_peers
        self.input_queue = input_queue
        self.output_processor = output_processor

        self.received_eof_signals = 0
        self.running = False

        self.supervisor_process = supervisor_process

    def run(self):
        for message in self.input_queue.read():
            message_type = message[:common.network.constants.HEADER_TYPE_LEN]
            message_body = message[common.network.constants.HEADER_TYPE_LEN:]
            result = self.process_input(message_type, message_body)
            if message_type == self.input_eof:
                self.register_eof()
            else:
                self.output_processor.process_output(result)

    def register_eof(self):
        self.received_eof_signals += 1
        if self.received_eof_signals == self.n_input_peers:
            self.output_processor.finish_processing()
            self.input_queue.close()
