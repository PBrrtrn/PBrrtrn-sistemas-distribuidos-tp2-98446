import common.network.constants
from common.rabbitmq.exchange_writer import ExchangeWriter


class ForwardingOutputProcessor:
    def __init__(self, n_output_peers: int, output_exchange_writer: ExchangeWriter, output_eof: bytes):
        self.n_output_peers = n_output_peers
        self.output_exchange_writer = output_exchange_writer
        self.output_eof = output_eof

    def process_output(self, channel, message: bytes, method, _properties):
        if message is not None:
            self.output_exchange_writer.write(message)
        #Commit de que se escribió el mensaje
        #ACK
        channel.basic_ack(delivery_tag=method.delivery_tag)


    def finish_processing(self, _result, _method, _properties):
        for _ in range(self.n_output_peers):
            self.output_exchange_writer.write(self.output_eof)
        # channel.basic_ack(delivery_tag=method.delivery_tag) No se debería hacer un ACK acá, no?
