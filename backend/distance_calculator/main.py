import common.env_utils
from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.exchange_writer import ExchangeWriter
from distance_calculator import DistanceCalculator

import common.env_utils


def main():
    config = common.env_utils.read_config()

    trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue_reader = QueueReader(
        queue_bindings=trips_input_queue_bindings,
        queue_name=config['TRIPS_INPUT_QUEUE_NAME']
    )

    trips_output_exchange_writer = ExchangeWriter(
        exchange_name=config['TRIPS_OUTPUT_EXCHANGE_NAME'],
        queue_name=config['TRIPS_OUTPUT_QUEUE_NAME']
    )

    n_montreal_stations_joiners = int(config['N_MONTREAL_STATIONS_JOINERS'])

    distance_calculator = DistanceCalculator(
        trips_input_queue_reader,
        trips_output_exchange_writer,
        n_montreal_stations_joiners
    )
    distance_calculator.run()


if __name__ == "__main__":
    main()
