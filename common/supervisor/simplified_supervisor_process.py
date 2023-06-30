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

        self.timers = {}
        self.missed_heartbeats = {}
        self.running = False

        self.process = multiprocessing.Process(target=self.__supervisor)

    def run(self):
        print(f"Running simplified supervisor process")
        self.process.start()

    def exit_gracefully(self, *_args):
        self.running = False
        print(f"INFO - Exiting gracefully")
        self.queue.close()
        self.process.join()

    def __supervisor(self):
        self.running = True

        self._refresh_all_peers()
        while self.running:
            if self._is_leader():
                self._leader()
            else:
                self._follower()

    def _is_leader(self):
        return self.node_id == self.network_size

    def _leader_id(self):
        return self.network_size

    def _refresh_all_peers(self):
        for peer_id in range(1, self.network_size + 1):
            self._refresh_peer(peer_id)

    def _refresh_peer(self, peer_id):
        self.timers[peer_id] = self.TIMEOUT
        self.missed_heartbeats[peer_id] = 0

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
            else:
                print(f"ERROR - Supervisor leader received unknown message type ({type_header}) from node {peer_id}")

        self._supervise_followers()

    def _follower(self):
        self.exchange_writer.write(
            message=common.supervisor.messages.heartbeat_message(self.node_id),
            routing_key=str(self._leader_id())
        )

        sleep(self.FOLLOWER_SLEEP_TIME + random.uniform(0, self.FOLLOWER_SLEEP_DELTA))

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
