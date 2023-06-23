import common.env_utils
from common.rabbitmq.queue import Queue
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.identity_process_input import identity_process_input_without_header
from common.rabbitmq.rpc_client import RPCClient
from storage_output_processor import StorageOutputProcessor
import common.network.constants



def main():
    config = common.env_utils.read_config()

    filtered_trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config['FILTERED_TRIPS_QUEUE_BINDINGS'])
    filtered_trips_input_queue_reader = Queue(
        hostname='rabbitmq',
        name=config['FILTERED_TRIPS_QUEUE_NAME'],
        bindings=filtered_trips_input_queue_bindings
    )

    requests_queue_reader = Queue(
        hostname='rabbitmq',
        name=config['DOUBLED_YEARLY_TRIPS_STATIONS_RPC_QUEUE_NAME']
    )
    stations_rpc_client = RPCClient(rpc_queue_name=config['STATIONS_RPC_QUEUE_NAME'])
    storage_output_processor = StorageOutputProcessor(requests_queue_reader, stations_rpc_client)

    processing_node = ProcessingNode(
        process_input=identity_process_input_without_header,
        input_eof=common.network.constants.TRIPS_END_ALL,
        n_input_peers=int(config['N_BY_YEAR_TRIPS_FILTERS']),
        input_queue=filtered_trips_input_queue_reader,
        output_processor=storage_output_processor
    )

    processing_node.run()



if __name__ == "__main__":
    main()
