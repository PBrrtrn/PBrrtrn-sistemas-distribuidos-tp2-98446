import os
from configparser import ConfigParser

import common.env_utils
from weather_manager import WeatherManager
from common.rabbitmq.queue_reader import QueueReader


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def main():
    config = read_config()

    input_queue_bindings = common.env_utils.parse_queue_bindings(config['FILTERED_WEATHER_QUEUE_BINDINGS'])
    input_queue_reader = QueueReader(
        queue_name=config['FILTERED_WEATHER_QUEUE_NAME'],
        queue_bindings=input_queue_bindings
    )

    n_weather_filters = config['N_WEATHER_FILTERS']
    rpc_queue_reader = QueueReader(queue_name=config['WEATHER_RPC_QUEUE_NAME'])
    weather_manager = WeatherManager(input_queue_reader, int(n_weather_filters), rpc_queue_reader)
    weather_manager.run()


if __name__ == "__main__":
    main()
