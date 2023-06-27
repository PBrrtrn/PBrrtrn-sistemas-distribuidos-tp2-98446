import common.env_utils
import common.supervisor.utils
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.rabbitmq.queue import Queue
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.processing_node.queue_consumer.output_processor.forwarding_output_processor import ForwardingOutputProcessor
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.queue_consumer.eof_handler import EOFHandler
from by_year_trips_filter_process_input import by_year_trips_filter_process_input
import common.network.constants


def main():
    config = common.env_utils.read_config()

    trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['TRIPS_INPUT_QUEUE_NAME'],
        bindings=trips_input_queue_bindings
    )

    filtered_trips_exchange_writer = ExchangeWriter(
        hostname=config['RABBITMQ_HOSTNAME'],
        queue_name=config['BY_STATION_AND_YEAR_TRIP_INPUT_QUEUE_NAME'],
        exchange_name=config['BY_STATION_AND_YEAR_TRIP_INPUT_EXCHANGE_NAME']
    )

    forwarding_output_processor = ForwardingOutputProcessor(
        n_output_peers=1,
        output_exchange_writer=filtered_trips_exchange_writer,
        output_eof=common.network.constants.TRIPS_END_ALL
    )

    queue_consumer = QueueConsumer(
        process_input=by_year_trips_filter_process_input,
        input_eof=common.network.constants.TRIPS_END_ALL,
        n_input_peers=1,
        input_queue=trips_input_queue,
        output_processor=forwarding_output_processor,
        eof_handler=EOFHandler('.eof')
    )

    processing_node = ProcessingNode(
        queue_consumer=queue_consumer,
        supervisor_process=common.supervisor.utils.create_from_config(config)
    )

    processing_node.run()


if __name__ == "__main__":
    main()
