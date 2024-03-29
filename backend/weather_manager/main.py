import os
from configparser import ConfigParser

import common.env_utils
import common.network.constants
import common.supervisor.utils
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer

from common.rabbitmq.queue import Queue
from common.processing_node.queue_consumer.process_input.identity_process_input import \
    identity_process_input
from common.processing_node.stateful_node import StatefulNode
from common.processing_node.queue_consumer.eof_handler import EOFHandler
from common.processing_node.queue_consumer.output_processor.storage_output_processor import StorageOutputProcessor
from rpc_weather_input_processor import RPCWeatherInputProcessor
from weather_storage_handler import WeatherStorageHandler


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def weather_manager_queue_consumer_factory(client_id: str, config):
    input_queue_bindings = common.env_utils.parse_queue_bindings_with_client_id(
        config['FILTERED_WEATHER_QUEUE_BINDINGS'], client_id)
    weather_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['FILTERED_WEATHER_QUEUE_NAME'] + client_id,
        bindings=input_queue_bindings
    )

    rpc_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['WEATHER_RPC_QUEUE_NAME'] + client_id
    )
    rpc_input_processor = RPCWeatherInputProcessor()
    storage_handler = WeatherStorageHandler(
        storage_directory=config['STORAGE_PATH'],
        checkpoint_frequency=int(config['CHECKPOINT_FREQUENCY']),
        client_id=client_id
    )
    storage_output_processor = StorageOutputProcessor(
        rpc_queue=rpc_queue,
        storage_handler=storage_handler,
        finish_processing_node_args={
            'input_eofs': [common.network.constants.TRIPS_END_ALL],
            'n_input_peers': 1,
            'rpc_input_processor': rpc_input_processor,
            'eof_handler': EOFHandler(".eof", append=f"_rpc_{client_id}")
        }
    )

    return QueueConsumer(
        process_input=identity_process_input,
        input_eofs=[common.network.constants.WEATHER_END_ALL],
        n_input_peers=int(config['N_WEATHER_FILTERS']),
        input_queue=weather_queue,
        output_processor=storage_output_processor,
        eof_handler=EOFHandler(".eof", append=f"_{client_id}")
    )


def main():
    config = read_config()
    new_clients_queue_bindings = common.env_utils.parse_queue_bindings(config['NEW_CLIENTS_QUEUE_BINDINGS'])

    new_clients_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['NEW_CLIENTS_QUEUE_NAME'],
        bindings=new_clients_queue_bindings,
        exchange_type='fanout'
    )
    processing_node = StatefulNode(
        supervisor_process=common.supervisor.utils.create_from_config(config),
        new_clients_queue=new_clients_queue,
        queue_consumer_factory=weather_manager_queue_consumer_factory,
        config=config
    )
    processing_node.run()


if __name__ == "__main__":
    main()
