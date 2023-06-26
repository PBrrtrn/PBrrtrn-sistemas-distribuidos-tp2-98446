from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.rabbitmq.queue import Queue
from common.processing_node.queue_consumer.output_processor.rpc_responder_output_processor import RPCResponderOutputProcessor
from common.processing_node.storage_handler import StorageHandler


class StorageOutputProcessor:
    def __init__(self, rpc_queue: Queue, storage_handler: StorageHandler,
                 finish_processing_node_args):
        self.storage_handler = storage_handler
        self.finish_processing_node_args = finish_processing_node_args
        self.rpc_queue = rpc_queue

    def process_output(self, channel, message: bytes, method, _properties):
        self.storage_handler.prepare(message)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.storage_handler.commit()
        # self.storage_handler.update_changes_in_disk()

    def finish_processing(self, _result, _method, _properties):
        rpc_input_processor = self.finish_processing_node_args['rpc_input_processor']
        rpc_input_processor.set_storage(self.storage_handler.get_storage())
        rpc_responder_output_processor = RPCResponderOutputProcessor(
            rpc_queue=self.rpc_queue,
            storage_handler=self.storage_handler
        )

        queue_consumer = QueueConsumer(
            process_input=rpc_input_processor.process_input,
            input_eof=self.finish_processing_node_args['input_eof'],
            n_input_peers=self.finish_processing_node_args['n_input_peers'],
            input_queue=self.rpc_queue,
            output_processor=rpc_responder_output_processor
        )

        queue_consumer.run()
