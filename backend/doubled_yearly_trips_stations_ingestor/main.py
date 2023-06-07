import common.env_utils

from doubled_yearly_trips_stations_ingestor import DoubledYearlyTripsStationsIngestor
from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.exchange_writer import ExchangeWriter


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_queue_reader = QueueReader(
        queue_name=config['TRIPS_INPUT_QUEUE_NAME'],
        queue_bindings=queue_bindings,
        exchange_type='fanout'
    )

    trips_exchange_writer = ExchangeWriter(
        exchange_name=config['TRIPS_OUTPUT_EXCHANGE_NAME'],
        queue_name=config['TRIPS_OUTPUT_QUEUE_NAME']
    )

    n_stations_joiners = int(config['N_BY_YEAR_TRIPS_FILTERS'])

    ingestor = DoubledYearlyTripsStationsIngestor(
        trips_queue_reader,
        trips_exchange_writer,
        n_stations_joiners
    )

    ingestor.run()


if __name__ == "__main__":
    main()
