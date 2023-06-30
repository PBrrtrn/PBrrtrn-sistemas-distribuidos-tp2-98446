from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.supervisor.supervisor_process import SupervisorProcess


class ProcessingNode:
    def __init__(self, queue_consumer: QueueConsumer, supervisor_process: SupervisorProcess):
        self.queue_consumer = queue_consumer
        self.supervisor_process = supervisor_process

    def run(self):
        # self.supervisor_process.run()
        self.queue_consumer.run()
