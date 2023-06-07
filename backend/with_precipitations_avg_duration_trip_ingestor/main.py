import common.env_utils
from with_precipitations_avg_duration_trip_ingestor import WithPrecipitationsAvgDurationTripIngestor
from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.rpc_client import RPCClient
from common.rabbitmq.exchange_writer import ExchangeWriter


def main():
    config = common.env_utils.read_config()

    trips_queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_queue_reader = QueueReader(
        queue_name=config['TRIPS_INPUT_QUEUE_NAME'],
        queue_bindings=trips_queue_bindings,
        exchange_type='fanout'
    )

    weather_rpc_client = RPCClient(config['WEATHER_JOIN_RPC_QUEUE_NAME'])

    running_avg_duration_exchange_writer = ExchangeWriter(
        exchange_name=config['RUNNING_AVG_DURATION_EXCHANGE_NAME'],
        queue_name=config['RUNNING_AVG_DURATION_QUEUE_NAME']
    )

    ingestor = WithPrecipitationsAvgDurationTripIngestor(
        trips_queue_reader,
        weather_rpc_client,
        running_avg_duration_exchange_writer
    )
    ingestor.run()


if __name__ == "__main__":
    main()
