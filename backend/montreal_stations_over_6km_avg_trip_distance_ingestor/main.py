import common.env_utils
import common.network.constants
import common.supervisor.utils
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer

from common.rabbitmq.queue import Queue
from common.processing_node.stateless_node import StatelessNode
from common.processing_node.queue_consumer.process_input.identity_process_input import identity_process_input
from common.processing_node.queue_consumer.output_processor.forwarding_output_processor import ForwardingOutputProcessor
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.processing_node.queue_consumer.eof_handler import EOFHandler


def main():
    config = common.env_utils.read_config()

    # queue_bindings = common.env_utils.parse_queue_bindings(config['TRIPS_INPUT_QUEUE_BINDINGS'])
    trips_input_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['TRIPS_INPUT_QUEUE_NAME'],
        bindings={'trips_exchange': ['']},
        exchange_type='fanout'
    )

    trips_exchange_writer = ExchangeWriter(
        hostname=config['RABBITMQ_HOSTNAME'],
        exchange_name=config['TRIPS_OUTPUT_EXCHANGE_NAME'],
        queue_name=config['TRIPS_OUTPUT_QUEUE_NAME']
    )

    output_processor = ForwardingOutputProcessor(
        n_output_peers=int(config['N_STATIONS_JOINERS']),
        output_exchange_writer=trips_exchange_writer,
        output_eof=common.network.constants.TRIPS_END_ALL,
        forward_with_routing_key=False
    )

    queue_consumer = QueueConsumer(
        process_input=identity_process_input,
        input_eofs=[common.network.constants.TRIPS_END_ALL],
        n_input_peers=1,
        input_queue=trips_input_queue,
        output_processor=output_processor,
        eof_handler=EOFHandler(".eof")
    )

    processing_node = StatelessNode(
        queue_consumer=queue_consumer,
        supervisor_process=common.supervisor.utils.create_from_config(config)
    )

    processing_node.run()


if __name__ == "__main__":
    main()
