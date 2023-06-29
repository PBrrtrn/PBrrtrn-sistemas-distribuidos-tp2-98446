import pickle

from common.supervisor.supervisor_process import SupervisorProcess
from common.rabbitmq.queue import Queue
import common.network.constants
from typing import Callable


class StatefulNode:
    def __init__(self, supervisor_process: SupervisorProcess, new_clients_queue: Queue,
                 queue_consumer_factory: Callable, config):
        self.supervisor_process = supervisor_process
        self.new_clients_queue = new_clients_queue
        self.queue_consumer_factory = queue_consumer_factory
        self.config = config

    def run(self):
        # self.supervisor_process.run()
        for (channel, method, properties, message) in self.new_clients_queue.read_with_props():
            # message_type = message[:common.network.constants.HEADER_TYPE_LEN]
            # client_id = message[common.network.constants.HEADER_TYPE_LEN:]
            client_id = pickle.loads(message)
            clients_queue = self.queue_consumer_factory(client_id, self.config)
            clients_queue.run()
            break
        # Duda: Joinear clientes viejos cada vez que se recibe un nuevo cliente,
            # O que por la cola manden que el cliente finalizó ?
            # register_new_client()
            # prepare()
            # ACK
            # commit()
            # new_client.run()
