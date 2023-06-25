import multiprocessing
import random
from time import sleep
from timeit import default_timer as timer

from common.supervisor.node_restarter import NodeRestarter
from common.supervisor.supervisor_queue import SupervisorQueue
import common.supervisor.messages

from common.rabbitmq.exchange_writer import ExchangeWriter


class SupervisorProcess:
    TIMEOUT = 1
    MAX_MISSED_HEARTBEATS = 3
    FOLLOWER_SLEEP_TIME = 0.1
    FOLLOWER_SLEEP_DELTA = 0.1

    def __init__(self,
                 exchange_writer: ExchangeWriter,
                 node_restarter: NodeRestarter,
                 queue: SupervisorQueue,
                 node_id: int,
                 network_size: int):
        self.exchange_writer = exchange_writer
        self.node_restarter = node_restarter
        self.queue = queue
        self.node_id = node_id
        self.network_size = network_size

        self.current_leader = None
        self.timers = {}
        self.missed_heartbeats = {}
        self.running = False

        self.process = multiprocessing.Process(target=self.__supervisor)

    def run(self):
        self.process.start()

    def exit_gracefully(self, *_args):
        self.running = False
        print(f"INFO - Exiting gracefully")
        self.queue.close()
        self.process.join()

    def __supervisor(self):
        self.running = True

        self._refresh_all_peers()
        self._run_election()
        while self.running:
            if self._is_leader():
                self._leader()
            else:
                self._follower()

    def _is_leader(self):
        return self.node_id == self.current_leader

    def _leader(self):
        start_time = timer()
        message = self.queue.read(timeout=self.TIMEOUT)
        end_time = timer()
        elapsed_time = end_time - start_time

        self._decrease_all_timers(elapsed_time)

        if message is not None:
            type_header, peer_id = common.supervisor.messages.parse_message(message)
            self._refresh_peer(peer_id)
            if type_header == common.supervisor.messages.HEARTBEAT:
                self._process_heartbeat(peer_id)
            elif type_header == common.supervisor.messages.ELECTION:
                self._handle_election_message(peer_id)
            else:
                print(f"ERROR - Received unknown message type ({type_header}) from node {peer_id}")

        self._supervise_followers()

    def _decrease_all_timers(self, elapsed_time: float):
        for peer_id in self.timers.keys():
            self.timers[peer_id] -= elapsed_time

    def _process_heartbeat(self, peer_id):
        self.exchange_writer.write(
            message=common.supervisor.messages.heartbeat_ack_message(self.node_id),
            routing_key=str(peer_id)
        )

    def _supervise_followers(self):
        for follower_id, remaining_time in self.timers.items():
            if remaining_time <= 0 and follower_id is not self.node_id:
                self.missed_heartbeats[follower_id] += 1
                self.timers[follower_id] = self.TIMEOUT
                self.exchange_writer.write(
                    message=common.supervisor.messages.heartbeat_ack_message(self.node_id),
                    routing_key=str(follower_id)
                )
                if self.missed_heartbeats[follower_id] >= self.MAX_MISSED_HEARTBEATS:
                    self.node_restarter.restart_node(follower_id)

    def _refresh_all_peers(self):
        for peer_id in range(1, self.network_size + 1):
            self._refresh_peer(peer_id)

    def _refresh_peer(self, peer_id):
        self.timers[peer_id] = self.TIMEOUT
        self.missed_heartbeats[peer_id] = 0

    def _follower(self):
        self.exchange_writer.write(
            message=common.supervisor.messages.heartbeat_message(self.node_id),
            routing_key=str(self.current_leader)
        )

        start_time = timer()
        response = self.queue.read(timeout=self.timers[self.current_leader])
        end_time = timer()
        elapsed_time = end_time - start_time

        self.timers[self.current_leader] -= elapsed_time

        if response is not None:
            type_header, peer_id = common.supervisor.messages.parse_message(response)
            if type_header == common.supervisor.messages.ELECTION:
                self._handle_election_message(peer_id)
            elif type_header == common.supervisor.messages.COORDINATOR:
                self._handle_coordinator_message(peer_id)
            elif type_header == common.supervisor.messages.HEARTBEAT_ACK:
                self._handle_heartbeat_ack()
            else:
                print(f"ERROR - Unknown message type (Got {type_header})")

        if self.timers[self.current_leader] <= 0:
            self.missed_heartbeats[self.current_leader] += 1
            if self.missed_heartbeats[self.current_leader] > self.MAX_MISSED_HEARTBEATS:
                self.missed_heartbeats[self.current_leader] = 0
                self._run_election()

        sleep(self.FOLLOWER_SLEEP_TIME + random.uniform(0, self.FOLLOWER_SLEEP_DELTA))

    def _handle_coordinator_message(self, peer_id: int):
        if peer_id > self.node_id:
            self._refresh_peer(peer_id)
            self.current_leader = peer_id

    def _handle_heartbeat_ack(self):
        self.timers[self.current_leader] = self.TIMEOUT
        self.missed_heartbeats[self.current_leader] = 0

    def _handle_election_message(self, peer_id: int):
        if peer_id < self.node_id:
            self.exchange_writer.write(
                message=common.supervisor.messages.answer_message(self.node_id),
                routing_key=str(peer_id)
            )

        self._run_election()

    def _run_election(self):
        if self._has_largest_id():
            self._announce_as_coordinator()
        else:
            # Enviar ELECTION a los nodos mayores y empezar a escuchar respuestas
            election_message = common.supervisor.messages.election_message(self.node_id)
            for node_id in range(self.node_id + 1, self.network_size + 1):
                self.exchange_writer.write(message=election_message, routing_key=str(node_id))

            total_elapsed_time = 0.0
            received_answer = False
            while True:
                start_time = timer()
                response = self.queue.read(timeout=self.TIMEOUT - total_elapsed_time)
                end_time = timer()

                if response is None and not received_answer:
                    # Si el read hace timeout, anunciarse como lider - los nodos mayores cayeron
                    self._announce_as_coordinator()
                    break

                response_type, response_node_id = common.supervisor.messages.parse_message(response)
                if response_type == common.supervisor.messages.ANSWER:
                    # Si llega un ANSWER, el nodo no participa más de la elección
                    received_answer = True
                elif response_type == common.supervisor.messages.COORDINATOR:
                    # Si llega un COORDINATOR, se termina la elección - los nodos mayores ya la resolvieron entre si
                    self.current_leader = response_node_id
                    break
                elif response_type == common.supervisor.messages.ELECTION:
                    # Si llega un ELECTION (nodos menores iniciaron eleccion), se responde y se reduce el timer
                    self.exchange_writer.write(
                        message=common.supervisor.messages.answer_message(self.node_id),
                        routing_key=str(response_node_id)
                    )

                    elapsed_time = end_time - start_time
                    total_elapsed_time += elapsed_time
                    if total_elapsed_time > self.TIMEOUT and not received_answer:
                        self._announce_as_coordinator()
                        break

    def _has_largest_id(self):
        return self.node_id == self.network_size

    def _announce_as_coordinator(self):
        self.current_leader = self.node_id
        coordinator_message = common.supervisor.messages.coordinator_message(self.node_id)
        self._refresh_all_peers()
        for peer_id in range(1, self.node_id):
            self.exchange_writer.write(message=coordinator_message, routing_key=str(peer_id))
