import os
from configparser import ConfigParser

import common.env_utils
from common.rabbitmq.queue import Queue
from common.processing_node.identity_process_input import identity_process_input_without_header
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.storage_output_processor import StorageOutputProcessor
from rpc_weather_input_processor import RPCWeatherInputProcessor
from weather_storage_handler import WeatherStorageHandler


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def main():
    config = read_config()

    input_queue_bindings = common.env_utils.parse_queue_bindings(config['FILTERED_WEATHER_QUEUE_BINDINGS'])
    weather_queue = Queue(
        hostname='rabbitmq',
        name=config['FILTERED_WEATHER_QUEUE_NAME'],
        bindings=input_queue_bindings
    )

    rpc_queue = Queue(
        hostname='rabbitmq',
        name=config['WEATHER_RPC_QUEUE_NAME']
    )
    rpc_input_processor = RPCWeatherInputProcessor()
    storage_handler = WeatherStorageHandler()
    storage_output_processor = StorageOutputProcessor(
        rpc_queue=rpc_queue,
        storage_handler=storage_handler,
        finish_processing_node_args={
            'input_eof': common.network.constants.EXECUTE_QUERIES,
            'n_input_peers': 1,
            'rpc_input_processor': rpc_input_processor
        }
    )

    processing_node = ProcessingNode(
        process_input=identity_process_input_without_header,
        input_eof=common.network.constants.WEATHER_END_ALL,
        n_input_peers=int(config['N_WEATHER_FILTERS']),
        input_queue=weather_queue,
        output_processor=storage_output_processor
    )
    processing_node.run()


if __name__ == "__main__":
    main()
