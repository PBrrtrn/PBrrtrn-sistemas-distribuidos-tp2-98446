import pickle
from multiprocessing import Process

from common.supervisor.simplified_supervisor_process import SupervisorProcess
from common.processing_node.queue_consumer.client_list_storage_handler import ClientListStorageHandler
from common.rabbitmq.queue import Queue
import common.network.constants
from typing import Callable


DIR = '.storage'
CLIENTS_LIST_FILENAME = 'clients_list'


class StatefulNode:
    def __init__(self, supervisor_process: SupervisorProcess, new_clients_queue: Queue,
                 queue_consumer_factory: Callable, config):
        self.supervisor_process = supervisor_process
        self.new_clients_queue = new_clients_queue
        self.queue_consumer_factory = queue_consumer_factory
        self.config = config
        self.supervisor_process = supervisor_process
        self.clients_list_handler = ClientListStorageHandler(storage_directory=DIR, filename=CLIENTS_LIST_FILENAME)
        current_client_list = self.clients_list_handler.get_clients_list()
        self.clients_queue_handler_dict = {}
        for client_id in current_client_list:
            self.clients_queue_handler_dict[client_id] = self.create_queue_consumer_process(client_id)

    def run(self):
        for client_id in self.clients_queue_handler_dict:
            self.clients_queue_handler_dict[client_id].start()

        self.supervisor_process.run()
        for (channel, method, properties, message) in self.new_clients_queue.read_with_props():
            # message_type = message[:common.network.constants.HEADER_TYPE_LEN]
            # client_id = message[common.network.constants.HEADER_TYPE_LEN:]
            client_id = pickle.loads(message)
            self.clients_list_handler.prepare(client_id)
            self.clients_queue_handler_dict[client_id] = self.create_queue_consumer_process(client_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.clients_list_handler.commit()
            self.clients_queue_handler_dict[client_id].start()
        # Duda: Joinear clientes viejos cada vez que se recibe un nuevo cliente,
        # O que por la cola manden que el cliente finalizó ?
        # register_new_client()
        # prepare()
        # ACK
        # commit()
        # new_client.run()

    def create_queue_consumer_process(self, client_id):
        clients_queue = self.queue_consumer_factory(client_id, self.config)
        client_process = Process(target=clients_queue.run, args=(), daemon=True)
        return client_process
