import common.env_utils
import common.network.constants
import common.supervisor.utils
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer

from common.rabbitmq.rpc_client import RPCClient
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.queue import Queue
from common.processing_node.stateless_node import StatelessNode
from common.processing_node.queue_consumer.output_processor.forwarding_output_processor import ForwardingOutputProcessor
from common.processing_node.queue_consumer.eof_handler import EOFHandler
from with_precipitation_input_processor import PrecipitationAvgDurationTripIngestorProcessor


def main():
    config = common.env_utils.read_config()

    trips_queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['TRIPS_INPUT_QUEUE_NAME'],
        bindings={'trips_exchange': trips_queue_bindings},
        exchange_type='fanout'
    )

    weather_rpc_client = RPCClient(config['WEATHER_JOIN_RPC_QUEUE_NAME'])
    input_processor = PrecipitationAvgDurationTripIngestorProcessor(weather_rpc_client=weather_rpc_client)

    running_avg_duration_exchange_writer = ExchangeWriter(
        hostname=config['RABBITMQ_HOSTNAME'],
        exchange_name=config['RUNNING_AVG_DURATION_EXCHANGE_NAME'],
        queue_name=config['RUNNING_AVG_DURATION_QUEUE_NAME']
    )

    output_processor = ForwardingOutputProcessor(
        n_output_peers=1,
        output_exchange_writer=running_avg_duration_exchange_writer,
        output_eof=common.network.constants.TRIPS_END_ALL,
        optional_rpc_eof=weather_rpc_client,
        forward_with_routing_key=True
    )

    queue_consumer = QueueConsumer(
        process_input=input_processor.process_input,
        input_eofs=[common.network.constants.TRIPS_END_ALL],
        n_input_peers=1,
        input_queue=trips_input_queue,
        output_processor=output_processor,
        eof_handler=EOFHandler(".eof", filename="eof_received")
    )

    processing_node = StatelessNode(
        queue_consumer=queue_consumer,
        supervisor_process=common.supervisor.utils.create_from_config(config)
    )

    processing_node.run()


if __name__ == "__main__":
    main()
