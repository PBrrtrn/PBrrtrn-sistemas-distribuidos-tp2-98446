import os
from configparser import ConfigParser

import common.env_utils
import common.network.constants
from common.rabbitmq.queue import Queue
from weather_manager_output_processor import WeatherManagerOutputProcessor
from common.processing_node.identity_process_input import identity_process_input
from common.processing_node.processing_node import ProcessingNode


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def main():
    config = read_config()

    input_queue_bindings = common.env_utils.parse_queue_bindings(config['FILTERED_WEATHER_QUEUE_BINDINGS'])
    weather_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['FILTERED_WEATHER_QUEUE_NAME'],
        bindings=input_queue_bindings
    )

    rpc_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['WEATHER_RPC_QUEUE_NAME']
    )

    weather_manager_output_processor = WeatherManagerOutputProcessor(rpc_queue=rpc_queue)
    processing_node = ProcessingNode(
        process_input=identity_process_input,
        input_eof=common.network.constants.WEATHER_END_ALL,
        n_input_peers=int(config['N_WEATHER_FILTERS']),
        input_queue=weather_queue,
        output_processor=weather_manager_output_processor
    )
    processing_node.run()


if __name__ == "__main__":
    main()
