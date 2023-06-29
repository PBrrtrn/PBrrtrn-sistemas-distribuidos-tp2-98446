from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.rabbitmq.queue import Queue
from common.processing_node.queue_consumer.output_processor.rpc_responder_output_processor import RPCResponderOutputProcessor
from common.processing_node.queue_consumer.output_processor.storage_handler import StorageHandler
import common.network.constants


class StorageOutputProcessor:
    def __init__(self, rpc_queue: Queue, storage_handler: StorageHandler,
                 finish_processing_node_args):
        self.storage_handler = storage_handler
        self.finish_processing_node_args = finish_processing_node_args
        self.rpc_queue = rpc_queue

    def process_output(self, channel, message: bytes, method, _properties, _client_id):
        message_body = message[common.network.constants.MESSAGE_HEADER_LEN:]
        self.storage_handler.prepare(message_body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.storage_handler.commit()
        # self.storage_handler.update_changes_in_disk()

    def finish_processing(self, client_id):
        rpc_input_processor = self.finish_processing_node_args['rpc_input_processor']
        rpc_input_processor.set_storage(self.storage_handler.get_storage())
        rpc_responder_output_processor = RPCResponderOutputProcessor(
            rpc_queue=self.rpc_queue,
            storage_handler=self.storage_handler,
            client_id=client_id,
            optional_rpc_eof=self.finish_processing_node_args.get('optional_rpc_eof', None),
            optional_rpc_eof_byte=self.finish_processing_node_args.get('optional_rpc_eof_byte', None)
        )

        queue_consumer = QueueConsumer(
            process_input=rpc_input_processor.process_input,
            input_eofs=self.finish_processing_node_args['input_eofs'],
            n_input_peers=self.finish_processing_node_args['n_input_peers'],
            input_queue=self.rpc_queue,
            output_processor=rpc_responder_output_processor,
            eof_handler=self.finish_processing_node_args['eof_handler']
        )

        queue_consumer.run()
