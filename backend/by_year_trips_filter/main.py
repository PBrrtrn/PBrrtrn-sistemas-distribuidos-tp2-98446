import common.env_utils
from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.exchange_writer import ExchangeWriter
from by_year_trips_filter import ByYearTripsFilter


def main():
    config = common.env_utils.read_config()

    trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue_reader = QueueReader(
        queue_name=config['TRIPS_INPUT_QUEUE_NAME'],
        queue_bindings=trips_input_queue_bindings
    )

    filtered_trips_exchange_writer = ExchangeWriter(
        queue_name=config['BY_STATION_AND_YEAR_TRIP_INPUT_QUEUE_NAME'],
        exchange_name=config['BY_STATION_AND_YEAR_TRIP_INPUT_EXCHANGE_NAME']
    )

    by_year_trips_filter = ByYearTripsFilter(trips_input_queue_reader, filtered_trips_exchange_writer)
    by_year_trips_filter.run()


if __name__ == "__main__":
    main()
