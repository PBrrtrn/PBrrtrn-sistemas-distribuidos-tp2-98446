from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.rpc_client import RPCClient
from common.rabbitmq.exchange_writer import ExchangeWriter

from montreal_stations_joiner import MontrealStationsJoiner

from common.env_utils import read_config
from common.env_utils import parse_queue_bindings


def main():
    config = read_config()

    trips_input_queue_bindings = parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue_reader = QueueReader(
        queue_bindings=trips_input_queue_bindings,
        queue_name=config['TRIPS_INPUT_QUEUE_NAME']
    )

    stations_join_rpc_client = RPCClient(rpc_queue_name=config['STATIONS_JOIN_RPC_QUEUE_NAME'])

    trips_output_exchange_writer = ExchangeWriter(
        exchange_name=config['JOINED_TRIPS_OUTPUT_EXCHANGE_NAME'],
        queue_name=config['JOINED_TRIPS_OUTPUT_QUEUE_NAME']
    )

    n_distance_calculators = int(config['N_DISTANCE_CALCULATORS'])

    stations_joiner = MontrealStationsJoiner(
        trips_input_queue_reader,
        stations_join_rpc_client,
        trips_output_exchange_writer,
        n_distance_calculators
    )
    stations_joiner.run()


if __name__ == "__main__":
    main()
