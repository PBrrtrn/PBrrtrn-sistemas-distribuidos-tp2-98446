import multiprocessing
import random
from time import sleep
from timeit import default_timer as timer

from common.supervisor.node_restarter import NodeRestarter
from common.supervisor.supervisor_queue import SupervisorQueue
import common.supervisor.messages

from common.rabbitmq.exchange_writer import ExchangeWriter


class SupervisorProcess:
    TIMEOUT = 5
    LEADER_RECEIVE_TIMEOUT = 1
    ACK_TIMEOUT = 1
    MAX_MISSED_HEARTBEATS = 3
    FOLLOWER_SLEEP_TIME = 0.1
    FOLLOWER_SLEEP_DELTA = 0.1

    def __init__(self,
                 exchange_writer: ExchangeWriter,
                 acks_exchange_writer: ExchangeWriter,
                 acks_queue: SupervisorQueue,
                 node_restarter: NodeRestarter,
                 queue: SupervisorQueue,
                 node_id: int,
                 network_size: int):
        self.exchange_writer = exchange_writer
        self.acks_exchange_writer = acks_exchange_writer
        self.node_restarter = node_restarter
        self.queue = queue
        self.acks_queue = acks_queue
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

        while self.running:
            if self._is_leader():
                self._leader()
            else:
                self._follower()

    def _leader(self):
        message, elapsed_time = self._read_message(self.LEADER_RECEIVE_TIMEOUT)

        self._decrease_all_timers(elapsed_time)
        if message:
            message_type, peer_id = common.supervisor.messages.parse_message(message)
            self._refresh_peer(peer_id)
            if message_type == common.supervisor.messages.COORDINATOR:
                self._handle_coordinator_message(peer_id)
            elif message_type == common.supervisor.messages.HEARTBEAT:
                self._process_heartbeat(peer_id)
            elif message_type == common.supervisor.messages.ELECTION:
                self._handle_election_message(peer_id)

        self._supervise_followers()

    def _decrease_all_timers(self, elapsed_time: float):
        for peer_id in self.timers.keys():
            self.timers[peer_id] -= elapsed_time

    def _process_heartbeat(self, peer_id):
        print(f"Got heartbeat from {peer_id}")
        self.exchange_writer.write(
            message=common.supervisor.messages.heartbeat_ack_message(self.node_id),
            routing_key=str(peer_id)
        )

    def _supervise_followers(self):
        for follower_id, remaining_time in self.timers.items():
            if remaining_time <= 0 and follower_id is not self.node_id:
                print(f"Follower {follower_id} missed a heartbeat")
                self.missed_heartbeats[follower_id] += 1
                self.timers[follower_id] = self.TIMEOUT
                self.exchange_writer.write(
                    message=common.supervisor.messages.heartbeat_ack_message(self.node_id),
                    routing_key=str(follower_id)
                )
                if self.missed_heartbeats[follower_id] >= self.MAX_MISSED_HEARTBEATS:
                    self.node_restarter.restart_node(follower_id)
                    self._refresh_peer(follower_id)

    def _follower(self):
        self.send_heartbeat()

        message, elapsed_time = self._read_message(timeout=self.timers[self.current_leader])
        self.timers[self.current_leader] -= elapsed_time
        if message:
            message_type, peer_id = common.supervisor.messages.parse_message(message)
            if message_type == common.supervisor.messages.COORDINATOR:
                self._handle_coordinator_message(peer_id)
            elif message_type == common.supervisor.messages.ELECTION:
                self._handle_election_message(peer_id)
            elif message_type == common.supervisor.messages.HEARTBEAT_ACK:
                self._handle_heartbeat_ack()

        if self.timers[self.current_leader] <= 0:
            print(f"Leader {self.current_leader} missed a heartbeat")
            self.missed_heartbeats[self.current_leader] += 1
            if self.missed_heartbeats[self.current_leader] > self.MAX_MISSED_HEARTBEATS:
                print(f"Leader is dead, must run election")
                self._refresh_peer(self.current_leader)
                self._run_election()

        sleep(self.FOLLOWER_SLEEP_TIME + random.uniform(0, self.FOLLOWER_SLEEP_DELTA))

    def send_heartbeat(self):
        self.exchange_writer.write(
            message=common.supervisor.messages.heartbeat_message(self.node_id),
            routing_key=str(self.current_leader)
        )

    def _handle_election_message(self, peer_id):
        print(f"Got ELECTION from {peer_id} (leader is {self.current_leader})")
        self._answer_election_message(peer_id)
        self._run_election()

    def _handle_heartbeat_ack(self):
        self.timers[self.current_leader] = self.TIMEOUT
        self.missed_heartbeats[self.current_leader] = 0

    def _run_election(self):
        self._send_election_message()

        self.current_leader = None
        while self.current_leader is None:
            response = self.queue.read(timeout=self.TIMEOUT)
            if response is None:
                self._announce_as_coordinator()
            else:
                response_type, peer_id = common.supervisor.messages.parse_message(response)
                if response_type == common.supervisor.messages.ELECTION:
                    self._answer_election_message(peer_id)
                elif response_type == common.supervisor.messages.COORDINATOR:
                    if self._handle_coordinator_message(peer_id):
                        self.current_leader = peer_id
                elif response_type == common.supervisor.messages.ANSWER:
                    self._await_coordinator_message()

    def _send_election_message(self):
        election_message = common.supervisor.messages.election_message(self.node_id)
        for peer_id in range(self.node_id + 1, self.network_size + 1):
            print(f"Sending election message to peer {peer_id}")
            self.exchange_writer.write(message=election_message, routing_key=str(peer_id))

    def _await_coordinator_message(self):
        while self.current_leader is None:
            message = self.queue.read(self.TIMEOUT)
            if message is not None:
                type_header, peer_id = common.supervisor.messages.parse_message(message)
                if type_header == common.supervisor.messages.COORDINATOR:
                    self._handle_coordinator_message(peer_id)

    def _handle_coordinator_message(self, peer_id):
        if peer_id >= self.node_id and (self.current_leader is None or peer_id >= self.current_leader):
            print(f"Sending ACK COORDINATOR to {peer_id}")
            self.acks_exchange_writer.write(common.supervisor.messages.coordinator_ack_message(self.node_id), routing_key=str(peer_id))
            self.current_leader = peer_id
            print(f"{peer_id} is new leader")
        else:
            print(f"Sending DENY COORDINATOR to {peer_id}")
            self.acks_exchange_writer.write(common.supervisor.messages.coordinator_deny_message(self.node_id), routing_key=str(peer_id))

    def _answer_election_message(self, peer_id):
        print(f"Answering ELECTION message for peer {peer_id}")
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
        coordinator_message = common.supervisor.messages.coordinator_message(self.node_id)
        for peer_id in range(1, self.network_size):
            print(f"Sending coordinator message to peer {peer_id}")
            self.exchange_writer.write(message=coordinator_message, routing_key=str(peer_id))
            message = self.acks_queue.read(self.ACK_TIMEOUT)

            if message is None:
                print(f"Received no response from coordinator announcement")
            else:
                message_type, peer_id = common.supervisor.messages.parse_message(message)
                if message_type == common.supervisor.messages.COORDINATOR_DENY:
                    print(f"RECEIVED DENY, node {self.node_id} cannot be leader")
                    return False
                else:
                    print(f"RECEIVED COORDINATOR ACK from {peer_id}")

        print(f"Node {self.node_id} will be the new leader")
        self.current_leader = self.node_id
        return True

    def _read_message(self, timeout):
        start_time = timer()
        message = self.queue.read(timeout=timeout)
        end_time = timer()
        elapsed_time = end_time - start_time

        return message, elapsed_time
