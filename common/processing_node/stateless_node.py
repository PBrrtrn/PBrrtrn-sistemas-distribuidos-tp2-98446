from multiprocessing import Process

from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.supervisor.supervisor_process import SupervisorProcess


class StatelessNode:
    def __init__(self, queue_consumer: QueueConsumer, supervisor_process: SupervisorProcess):
        self.queue_consumer = queue_consumer
        self.supervisor = supervisor_process

    def run(self):
        supervisor_process = Process(target=self.supervisor.run, args=(), daemon=True)
        supervisor_process.start()
        self.queue_consumer.run()
