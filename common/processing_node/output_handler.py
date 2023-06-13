from common.rabbitmq.exchange_writer import ExchangeWriter


class OutputHandler:
    def __init__(self, n_output_peers: int, output_exchange_writer: ExchangeWriter):
        self.n_output_peers = n_output_peers
        self.output_exchange_writer = output_exchange_writer

    def output_message(self, message: bytes):
        self.output_exchange_writer.write(message)

    def output_eof(self, node_id):
        for _ in range(self.n_output_peers):
            self.output_exchange_writer.write()