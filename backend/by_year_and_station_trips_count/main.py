import common.env_utils

from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.rpc_client import RPCClient

from by_year_and_station_trips_count import ByYearAndStationTripsCount


def main():
    config = common.env_utils.read_config()

    filtered_trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config['FILTERED_TRIPS_QUEUE_BINDINGS'])
    filtered_trips_input_queue_reader = QueueReader(
        queue_name=config['FILTERED_TRIPS_QUEUE_NAME'],
        queue_bindings=filtered_trips_input_queue_bindings
    )

    requests_queue_reader = QueueReader(queue_name=config['DOUBLED_YEARLY_TRIPS_STATIONS_RPC_QUEUE_NAME'])
    stations_rpc_client = RPCClient(rpc_queue_name=config['STATIONS_RPC_QUEUE_NAME'])
    n_by_year_trips_filters = int(config['N_BY_YEAR_TRIPS_FILTERS'])

    by_year_and_station_trips_count = ByYearAndStationTripsCount(
        filtered_trips_input_queue_reader,
        requests_queue_reader,
        stations_rpc_client,
        n_by_year_trips_filters
    )

    by_year_and_station_trips_count.run()


if __name__ == "__main__":
    main()
