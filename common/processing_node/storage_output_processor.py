from common.rabbitmq.queue import Queue
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.rpc_responder_output_processor import RPCResponderOutputProcessor
from storage_handler import StorageHandler


class StorageOutputHandler:
    def __init__(self, rpc_queue: Queue, storage_handler: StorageHandler, rpc_input_processor):
        self.storage_handler = storage_handler
        self.rpc_input_processor = rpc_input_processor
        self.rpc_queue = rpc_queue

    def process_output(self, message: bytes):
        self.storage_handler.prepare(message)
        #ACK
        self.storage_handler.commit()
        self.storage_handler.update_changes_in_disk()

    def finish_processing(self, _result, _method, _properties):
        self.rpc_input_processor.set_storage(self.storage_handler.get_storage())
        rpc_responder_output_processor = RPCResponderOutputProcessor(self.rpc_queue)
        processing_node = ProcessingNode(
            process_input=self.rpc_input_processor.process_rpc,
            input_eof=common.network.constants.EXECUTE_QUERIES,
            n_input_peers=int(config['N_BY_YEAR_TRIPS_FILTERS']),
            input_queue=self.rpc_queue,
            output_processor=rpc_responder_output_processor
        )
        processing_node.run()
