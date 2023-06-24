import common.env_utils
import common.network.constants
from common.rabbitmq.queue import Queue
from common.processing_node.identity_process_input import identity_process_input_without_header
from common.processing_node.processing_node import ProcessingNode
from trip_duration_storage_output_processor import TripDurationStorageOutputProcessor


def main():
    config = common.env_utils.read_config()

    trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config["TRIPS_INPUT_QUEUE_BINDINGS"])
    trips_input_queue_name = config["TRIPS_INPUT_QUEUE_NAME"]
    trips_input_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=trips_input_queue_name,
        bindings=trips_input_queue_bindings
    )

    rpc_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config["RPC_QUEUE_NAME"]
    )

    storage_output_processor = TripDurationStorageOutputProcessor(rpc_queue_reader)

    processing_node = ProcessingNode(
        process_input=identity_process_input_without_header,
        input_eof=common.network.constants.TRIPS_END_ALL,
        n_input_peers=1,
        input_queue=trips_input_queue_reader,
        output_processor=storage_output_processor
    )

    processing_node.run()


if __name__ == "__main__":
    main()
