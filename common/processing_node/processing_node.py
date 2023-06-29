from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.supervisor.supervisor_process import SupervisorProcess


class ProcessingNode:
    def __init__(self, queue_consumer: QueueConsumer, supervisor_process: SupervisorProcess):
        self.queue_consumer = queue_consumer
        self.supervisor_process = supervisor_process

    def run(self):
        #self.supervisor_process.run()
        #while True:
            #Popear de la cola
            #Preparar cliente (Instanciar queueconsumer process), escribir en un archivo un log con los
    # ids de los clientes actualizadas
    #Commit
            #ACK
        self.queue_consumer.run()
        #Lista de queue consumers self.queue_consumer.run()
