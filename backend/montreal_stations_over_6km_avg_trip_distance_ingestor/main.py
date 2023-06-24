import common.env_utils
import common.network.constants

from common.rabbitmq.queue import Queue
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.identity_process_input import identity_process_input
from common.processing_node.forwarding_output_processor import ForwardingOutputProcessor
from common.rabbitmq.exchange_writer import ExchangeWriter


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['TRIPS_INPUT_QUEUE_NAME'],
        bindings=queue_bindings,
        exchange_type='fanout'
    )

    trips_exchange_writer = ExchangeWriter(
        hostname=config['RABBITMQ_HOSTNAME'],
        exchange_name=config['TRIPS_OUTPUT_EXCHANGE_NAME'],
        queue_name=config['TRIPS_OUTPUT_QUEUE_NAME']
    )

    output_processor = ForwardingOutputProcessor(
        n_output_peers=int(config['N_STATIONS_JOINERS']),
        output_exchange_writer=trips_exchange_writer,
        output_eof=common.network.constants.TRIPS_END_ALL
    )

    processing_node = ProcessingNode(
        process_input=identity_process_input,
        input_eof=common.network.constants.TRIPS_END_ALL,
        n_input_peers=1,
        input_queue=trips_input_queue,
        output_processor=output_processor
    )

    processing_node.run()


if __name__ == "__main__":
    main()
