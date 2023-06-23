import common.env_utils
from with_precipitations_avg_duration_trip_ingestor import WithPrecipitationsAvgDurationTripIngestor
from common.rabbitmq.rpc_client import RPCClient
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.queue import Queue
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.forwarding_output_processor import ForwardingOutputProcessor
from with_precipitation_input_processor import PrecipitationAvgDurationTripIngestorProcessor

def main():
    config = common.env_utils.read_config()

    trips_queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue = Queue(
        hostname='rabbitmq',
        name=config['TRIPS_INPUT_QUEUE_NAME'],
        bindings=trips_queue_bindings,
        exchange_type='fanout'
    )

    weather_rpc_client = RPCClient(config['WEATHER_JOIN_RPC_QUEUE_NAME'])
    input_processor = PrecipitationAvgDurationTripIngestorProcessor(weather_rpc_client=weather_rpc_client)

    running_avg_duration_exchange_writer = ExchangeWriter(
        exchange_name=config['RUNNING_AVG_DURATION_EXCHANGE_NAME'],
        queue_name=config['RUNNING_AVG_DURATION_QUEUE_NAME']
    )

    output_processor = ForwardingOutputProcessor(
        n_output_peers=1,
        output_exchange_writer=running_avg_duration_exchange_writer,
        output_eof=common.network.constants.TRIPS_END_ALL
    )

    processing_node = ProcessingNode(
        process_input=input_processor.process_input,
        input_eof=common.network.constants.TRIPS_END_ALL,
        n_input_peers=1,
        input_queue=trips_input_queue,
        output_processor=output_processor
    )

    processing_node.run()


if __name__ == "__main__":
    main()
