import os
from configparser import ConfigParser

import common.env_utils
import common.network.constants

from common.rabbitmq.queue import Queue
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.processing_node.forwarding_output_processor import ForwardingOutputProcessor
from common.processing_node.processing_node import ProcessingNode

from weather_filter_process_input import weather_filter_process_input


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['UNFILTERED_WEATHER_QUEUE_BINDINGS'])
    input_queue_reader = Queue(
        hostname='rabbitmq',
        name=config['UNFILTERED_WEATHER_QUEUE_NAME'],
        bindings=queue_bindings
    )

    output_exchange_writer = ExchangeWriter(
        exchange_name=config['FILTERED_WEATHER_EXCHANGE_NAME'],
        queue_name=config['FILTERED_WEATHER_QUEUE_NAME']
    )

    forwarding_output_processor = ForwardingOutputProcessor(
        n_output_peers=1,
        output_exchange_writer=output_exchange_writer,
        output_eof=common.network.constants.WEATHER_END_ALL
    )

    processing_node = ProcessingNode(
        process_input=weather_filter_process_input,
        input_eof=common.network.constants.WEATHER_END_ALL,
        n_input_peers=1,
        input_queue=input_queue_reader,
        output_processor=forwarding_output_processor
    )

    processing_node.run()


if __name__ == "__main__":
    main()
