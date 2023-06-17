import common.env_utils
from common.rabbitmq.queue import Queue
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.processing_node.forwarding_output_processor import ForwardingOutputProcessor
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.filter_process_input import FilterProcessInput


def filter_function(trips_batch):
    print("pepeee")
    filtered_trips = []
    for trip in trips_batch:
        if trip.start_date[:4] == "2017" or trip.start_date[:4] == "2016":
            filtered_trips.append(trip)
    return filtered_trips


def main():
    config = common.env_utils.read_config()

    trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue = Queue(
        hostname='rabbitmq',
        name=config['TRIPS_INPUT_QUEUE_NAME'],
        bindings=trips_input_queue_bindings
    )

    filtered_trips_exchange_writer = ExchangeWriter(
        queue_name=config['BY_STATION_AND_YEAR_TRIP_INPUT_QUEUE_NAME'],
        exchange_name=config['BY_STATION_AND_YEAR_TRIP_INPUT_EXCHANGE_NAME']
    )

    output_processor = ForwardingOutputProcessor(
        n_output_peers=1,
        output_exchange_writer=filtered_trips_exchange_writer,
        output_eof=common.network.constants.TRIPS_END_ALL
    )

    filter_process_input = FilterProcessInput(filter_function)
    processing_node = ProcessingNode(
        process_input=filter_process_input.filter_process_input,
        input_eof=common.network.constants.TRIPS_END_ALL,
        n_input_peers=1,
        input_queue=trips_input_queue,
        output_processor=output_processor
    )

    processing_node.run()


if __name__ == "__main__":
    main()
