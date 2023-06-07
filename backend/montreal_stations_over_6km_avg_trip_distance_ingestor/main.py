import common.env_utils

from montreal_stations_over_6km_avg_trip_distance_ingestor import MontrealStationsOver6KmAvgTripDistanceIngestor
from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.rpc_client import RPCClient
from common.rabbitmq.exchange_writer import ExchangeWriter


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_queue_reader = QueueReader(
        queue_name=config['TRIPS_INPUT_QUEUE_NAME'],
        queue_bindings=queue_bindings,
        exchange_type='fanout')

    trips_exchange_writer = ExchangeWriter(
        exchange_name=config['TRIPS_OUTPUT_EXCHANGE_NAME'],
        queue_name=config['TRIPS_OUTPUT_QUEUE_NAME']
    )

    n_stations_joiners = int(config['N_STATIONS_JOINERS'])

    ingestor = MontrealStationsOver6KmAvgTripDistanceIngestor(
        trips_queue_reader,
        trips_exchange_writer,
        n_stations_joiners
    )

    ingestor.run()


if __name__ == "__main__":
    main()
