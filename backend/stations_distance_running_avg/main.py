import common.env_utils
import common.network.constants
import common.supervisor.utils
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer

from common.rabbitmq.queue import Queue
from common.processing_node.queue_consumer.process_input.identity_process_input import identity_process_input_without_header
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.queue_consumer.output_processor.storage_output_processor import StorageOutputProcessor
from station_distance_storage_handler import StationDistanceStorageHandler
from rpc_distance_input_processor import RPCDistanceInputProcessor


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['STATIONS_TRIP_DISTANCE_INPUT_QUEUE_BINDINGS'])
    stations_trip_distance_input_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['STATIONS_TRIP_DISTANCE_INPUT_QUEUE_NAME'],
        bindings=queue_bindings
    )

    rpc_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['QUERY_RPC_QUEUE_NAME']
    )
    rpc_input_processor = RPCDistanceInputProcessor()
    storage_handler = StationDistanceStorageHandler(
        storage_directory=config['STORAGE_PATH'],
        checkpoint_frequency=int(config['CHECKPOINT_FREQUENCY'])
    )
    storage_output_processor = StorageOutputProcessor(
        rpc_queue=rpc_queue_reader,
        storage_handler=storage_handler,
        finish_processing_node_args={
            'input_eof': common.network.constants.EXECUTE_QUERIES,
            'n_input_peers': 1,
            'rpc_input_processor': rpc_input_processor
        }
    )

    queue_consumer = QueueConsumer(
        process_input=identity_process_input_without_header,
        input_eof=common.network.constants.TRIPS_END_ALL,
        n_input_peers=int(config['N_DISTANCE_CALCULATORS']),
        input_queue=stations_trip_distance_input_queue_reader,
        output_processor=storage_output_processor
    )

    processing_node = ProcessingNode(
        queue_consumer=queue_consumer,
        supervisor_process=common.supervisor.utils.create_from_config(config)
    )

    processing_node.run()


if __name__ == "__main__":
    main()
