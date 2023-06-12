from timeit import default_timer as timer

from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.queue import Queue

import messages


class BullyNode:
    ELECTION_TIMEOUT = 0.5

    def __init__(self, exchange_writer: ExchangeWriter, queue: Queue, node_id: int, network_size: int):
        self.exchange_writer = exchange_writer
        self.queue = queue
        self.node_id = node_id
        self.network_size = network_size
        self.current_leader = None
        self.running = False

    def start(self):
        self.running = True

        self.run_election()

        # Loop de recibir mensajes
        while self.running:
            message = self.queue.read()

            if message is not None:
                type_header, peer_id = messages.parse_message(message)

                if type_header == messages.COORDINATOR:
                    self.handle_coordinator_message(peer_id)
                elif type_header == messages.ELECTION:
                    self.handle_election_message(peer_id)
                else:
                    print(f"ERROR - Unknown message type (Got {type_header})")

    def run_election(self):
        received_response = False
        if not self.has_largest_id():
            # Enviar ELECTION a los nodos mayores y empezar a escuchar respuestas
            election_message = messages.election_message(self.node_id)
            for node_id in range(self.node_id + 1, self.network_size):
                self.exchange_writer.write(message=election_message, routing_key=str(node_id))

            total_elapsed_time = 0.0
            while True:
                start_time = timer()
                response = self.queue.read(timeout=self.ELECTION_TIMEOUT - total_elapsed_time)
                end_time = timer()

                if response is None:
                    # Si el read hace timeout, anunciarse como lider - los nodos mayores cayeron
                    self.announce_as_coordinator()
                    break

                response_type, response_node_id = messages.parse_message(response)
                if response_type == messages.ANSWER:
                    # Si llega un ANSWER, el nodo no participa más de la elección
                    break
                elif response_type == messages.COORDINATOR:
                    # Si llega un COORDINATOR, se termina la elección - los nodos mayores ya la resolvieron entre si
                    print(f"INFO - Got coordinator message from peer #{response_node_id}")
                    print(f"INFO - Peer #{response_node_id} is the new leader, ALL HAIL PEER #{response_node_id}!")
                    self.current_leader = response_node_id
                    break
                elif response_type == messages.ELECTION:
                    # Si llega un ELECTION (nodos menores iniciaron eleccion), se responde y se reduce el timer
                    print(f"INFO - Got election message from peer #{response_node_id}")
                    print(f"INFO - Peer #{response_node_id} is not the new leader, SIT DOWN, #{response_node_id}!")
                    self.exchange_writer.write(
                        message=messages.answer_message(self.node_id),
                        routing_key=str(response_node_id)
                    )

                    elapsed_time = end_time - start_time
                    total_elapsed_time += elapsed_time
                    if total_elapsed_time > self.ELECTION_TIMEOUT:
                        self.announce_as_coordinator()
                        break

    def has_largest_id(self):
        return self.node_id + 1 == self.network_size

    def announce_as_coordinator(self):
        print(f"INFO - Looks like node #{self.node_id} the captain of this ship now")
        message = messages.coordinator_message(self.node_id)
        for peer_id in range(0, self.node_id):
            self.exchange_writer.write(message=message, routing_key=str(peer_id))

    def handle_coordinator_message(self, peer_id: int):
        print(f"INFO - Got coordinator message from peer #{peer_id}")
        if peer_id > self.node_id:
            print(f"INFO - Peer #{peer_id} is the new leader, ALL HAIL PEER #{peer_id}!")
            self.current_leader = peer_id

    def handle_election_message(self, peer_id: int):
        print(f"INFO - Got election message from peer #{peer_id}")
        if peer_id < self.node_id:
            print(f"INFO - Peer #{peer_id} is not the new leader, SIT DOWN, #{peer_id}!")
            self.exchange_writer.write(
                message=messages.answer_message(self.node_id),
                routing_key=str(peer_id)
            )

        self.run_election()




