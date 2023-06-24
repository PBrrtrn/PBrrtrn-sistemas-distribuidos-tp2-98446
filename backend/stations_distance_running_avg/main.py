import common.env_utils
import common.network.constants
import common.supervisor.utils

from common.rabbitmq.queue import Queue
from common.processing_node.identity_process_input import identity_process_input_without_header
from common.processing_node.processing_node import ProcessingNode
from station_distance_storage_output_processor import StationDistanceStorageOutputProcessor


def main():
    """config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['STATIONS_TRIP_DISTANCE_INPUT_QUEUE_BINDINGS'])
    stations_trip_distance_input_queue_reader = QueueReader(
        queue_name=config['STATIONS_TRIP_DISTANCE_INPUT_QUEUE_NAME'],
        queue_bindings=queue_bindings)

    rpc_queue_reader = QueueReader(queue_name=config['QUERY_RPC_QUEUE_NAME'])

    n_distance_calculators = int(config['N_DISTANCE_CALCULATORS'])

    stations_distance_running_avg = StationsDistanceRunningAvg(
        stations_trip_distance_input_queue_reader,
        rpc_queue_reader,
        n_distance_calculators
    )

    stations_distance_running_avg.run()"""
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
    storage_output_processor = StationDistanceStorageOutputProcessor(rpc_queue_reader)

    processing_node = ProcessingNode(
        process_input=identity_process_input_without_header,
        input_eof=common.network.constants.TRIPS_END_ALL,
        n_input_peers=int(config['N_DISTANCE_CALCULATORS']),
        input_queue=stations_trip_distance_input_queue_reader,
        output_processor=storage_output_processor,
        supervisor_process=common.supervisor.utils.create_from_config(config)
    )

    processing_node.run()


if __name__ == "__main__":
    main()
