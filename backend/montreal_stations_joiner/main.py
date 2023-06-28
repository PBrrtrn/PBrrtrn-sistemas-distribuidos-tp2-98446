from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.rabbitmq.rpc_client import RPCClient
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.queue import Queue
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.queue_consumer.output_processor.forwarding_output_processor import ForwardingOutputProcessor
from montreal_joiner_input_processor import MontrealJoinerInputProcessor
from common.processing_node.queue_consumer.eof_handler import EOFHandler

import common.network.constants
import common.supervisor.utils

from common.env_utils import read_config
from common.env_utils import parse_queue_bindings


def main():
    config = read_config()

    trips_input_queue_bindings = parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        bindings=trips_input_queue_bindings,
        name=config['TRIPS_INPUT_QUEUE_NAME']
    )

    stations_join_rpc_client = RPCClient(rpc_queue_name=config['STATIONS_JOIN_RPC_QUEUE_NAME'])

    input_processor = MontrealJoinerInputProcessor(stations_join_rpc_client)

    trips_output_exchange_writer = ExchangeWriter(
        hostname=config['RABBITMQ_HOSTNAME'],
        exchange_name=config['JOINED_TRIPS_OUTPUT_EXCHANGE_NAME'],
        queue_name=config['JOINED_TRIPS_OUTPUT_QUEUE_NAME']
    )

    output_processor = ForwardingOutputProcessor(
        n_output_peers=int(config['N_DISTANCE_CALCULATORS']),
        output_exchange_writer=trips_output_exchange_writer,
        output_eof=common.network.constants.TRIPS_END_ALL
    )

    queue_consumer = QueueConsumer(
        process_input=input_processor.process_input,
        input_eofs=[common.network.constants.TRIPS_END_ALL],
        n_input_peers=1,
        input_queue=trips_input_queue,
        output_processor=output_processor,
        eof_handler=EOFHandler(".eof")
    )

    processing_node = ProcessingNode(
        queue_consumer=queue_consumer,
        supervisor_process=common.supervisor.utils.create_from_config(config)
    )

    processing_node.run()


if __name__ == "__main__":
    main()
