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
        self.supervisor_process.run()
        queue_consumer_process = Process(target=self.queue_consumer.run, args=(), daemon=True)
        queue_consumer_process.start()
        for (channel, method, properties, message) in self.new_clients_queue.read_with_props():
            client_id = pickle.loads(message)
            print(f"New client {client_id}.")
            current_client_list = self.clients_list_handler.get_clients_list()
            if client_id in current_client_list:
                continue
            self.clients_list_handler.prepare(client_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.clients_list_handler.commit()
