import os
from configparser import ConfigParser

import common.env_utils

from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.exchange_writer import ExchangeWriter

from weather_filter import WeatherFilter


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def main():
    config = read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['UNFILTERED_WEATHER_QUEUE_BINDINGS'])
    input_queue_reader = QueueReader(
        queue_name=config["UNFILTERED_WEATHER_QUEUE_NAME"],
        queue_bindings=queue_bindings
    )

    output_exchange_writer = ExchangeWriter(
        exchange_name=config["FILTERED_WEATHER_EXCHANGE_NAME"],
        queue_name=config["FILTERED_WEATHER_QUEUE_NAME"]
    )

    weather_filter = WeatherFilter(input_queue_reader, output_exchange_writer)
    weather_filter.run()


if __name__ == "__main__":
    main()
