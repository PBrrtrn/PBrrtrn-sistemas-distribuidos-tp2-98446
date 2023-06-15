import common.network.constants
from common.rabbitmq.exchange_writer import ExchangeWriter


class ForwardingOutputProcessor:
    def __init__(self, n_output_peers: int, output_exchange_writer: ExchangeWriter, output_eof: bytes):
        self.n_output_peers = n_output_peers
        self.output_exchange_writer = output_exchange_writer
        self.output_eof = output_eof

    def process_output(self, message: bytes):
        self.output_exchange_writer.write(message)

    def finish_processing(self):
        for _ in range(self.n_output_peers):
            self.output_exchange_writer.write(self.output_eof)
