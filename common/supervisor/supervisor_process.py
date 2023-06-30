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
        self.running = True
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

        if self._is_leader():  # TODO: while self.running:
            self._leader()
        else:
            self._follower()

    def _leader(self):
        print("I'm leader!")

    def _follower(self):
        print(f"I'm follower, leader is {self.current_leader}")

    def _run_election(self):
        self._send_election_message()

        while True:
            response = self.queue.read(timeout=self.TIMEOUT)
            if response is None:
                self._announce_as_coordinator()
                break
            else:
                response_type, peer_id = common.supervisor.messages.parse_message(response)
                if response_type == common.supervisor.messages.ELECTION:
                    self._answer_election_message(peer_id)
                if response_type == common.supervisor.messages.COORDINATOR:
                    self.current_leader = peer_id
                    break
                if response_type == common.supervisor.messages.ANSWER:
                    self._await_coordinator_message()
                    break

    def _send_election_message(self):
        election_message = common.supervisor.messages.election_message(self.node_id)
        for peer_id in range(self.node_id + 1, self.network_size + 1):
            print(f"Sending election message to peer {peer_id}")
            self.exchange_writer.write(message=election_message, routing_key=str(peer_id))

    def _await_coordinator_message(self):
        while True:
            message = self.queue.read(self.TIMEOUT)
            if message is not None:
                type_header, peer_id = common.supervisor.messages.parse_message(message)
                if type_header == common.supervisor.messages.COORDINATOR:
                    self.current_leader = peer_id
                    break

    def _answer_election_message(self, peer_id):
        print(f"Answering election message for peer {peer_id}")
        self.exchange_writer.write(
            message=common.supervisor.messages.answer_message(self.node_id),
            routing_key=str(peer_id)
        )

    def _refresh_all_peers(self):
        for peer_id in range(1, self.network_size + 1):
            self._refresh_peer(peer_id)

    def _refresh_peer(self, peer_id):
        self.timers[peer_id] = self.TIMEOUT
        self.missed_heartbeats[peer_id] = 0

    def _is_leader(self):
        return self.node_id == self.current_leader

    def _announce_as_coordinator(self):
        self.current_leader = self.node_id
        coordinator_message = common.supervisor.messages.coordinator_message(self.node_id)
        for peer_id in range(1, self.node_id):
            print(f"Sending coordinator message to peer {peer_id}")
            self.exchange_writer.write(message=coordinator_message, routing_key=str(peer_id))

    def _read_message(self, timeout):
        start_time = timer()
        message = self.queue.read(timeout=timeout)
        end_time = timer()
        elapsed_time = end_time - start_time

        return message, elapsed_time
