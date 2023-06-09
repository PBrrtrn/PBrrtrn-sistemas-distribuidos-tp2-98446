from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.queue_reader import QueueReader

import messages


class BullyNode:
    def __init__(self, exchange_writer: ExchangeWriter, queue_reader: QueueReader, node_id: int, n_peers: int):
        self.exchange_writer = exchange_writer
        self.queue_reader = queue_reader
        self.node_id = node_id
        self.n_peers = n_peers
        self.current_leader = None

    def start(self):
        # Proceso empieza enviando Election a nodos con ID mayor
        for peer_id in range(self.node_id + 1, self.n_peers):
            self.exchange_writer.write(message=messages.election_message(self.node_id), routing_key=str(peer_id))

        # Si no recibe respuesta, se autoproclama lider
        if False:
            self.current_leader = self.node_id

        self.queue_reader.consume(self.receive_loop, auto_ack=False)

    def receive_loop(self):

