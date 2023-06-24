import common.env_utils
from distance_calculator_process_input import distance_calculator_process_input
from common.processing_node.forwarding_output_processor import ForwardingOutputProcessor
from common.processing_node.processing_node import ProcessingNode
from common.rabbitmq.queue import Queue
import common.network.constants
from common.rabbitmq.exchange_writer import ExchangeWriter

import common.env_utils


def main():
    config = common.env_utils.read_config()

    trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue = Queue(
        hostname='rabbitmq',
        name=config['TRIPS_INPUT_QUEUE_NAME'],
        bindings=trips_input_queue_bindings
    )

    trips_output_exchange_writer = ExchangeWriter(
        exchange_name=config['TRIPS_OUTPUT_EXCHANGE_NAME'],
        queue_name=config['TRIPS_OUTPUT_QUEUE_NAME']
    )

    output_processor = ForwardingOutputProcessor(
        n_output_peers=1,
        output_exchange_writer=trips_output_exchange_writer,
        output_eof=common.network.constants.TRIPS_END_ALL
    )

    processing_node = ProcessingNode(
        process_input=distance_calculator_process_input,
        input_eof=common.network.constants.TRIPS_END_ALL,
        n_input_peers=int(config['N_MONTREAL_STATIONS_JOINERS']),
        input_queue=trips_input_queue,
        output_processor=output_processor
    )

    processing_node.run()


if __name__ == "__main__":
    main()
