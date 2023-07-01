import pickle
from multiprocessing import Process

from common.rabbitmq.queue import Queue
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.supervisor.simplified_supervisor_process import SupervisorProcess
from common.processing_node.queue_consumer.client_list_storage_handler import ClientListStorageHandler


class StatelessNode:
    def __init__(self, queue_consumer: QueueConsumer, supervisor_process: SupervisorProcess, new_clients_queue: Queue,
                 clients_list_handler: ClientListStorageHandler):
        self.queue_consumer = queue_consumer
        self.supervisor_process = supervisor_process
        self.new_clients_queue = new_clients_queue
        self.clients_list_handler = clients_list_handler

    def run(self):
        #self.supervisor_process.run()
        self.queue_consumer.run()
