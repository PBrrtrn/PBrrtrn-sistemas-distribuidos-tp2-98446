import os
from configparser import ConfigParser

from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.rpc_client import RPCClient
from data_ingestion_server import DataIngestionServer


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def main():
    config = read_config()

    stations_exchange_writer = ExchangeWriter(
        hostname=config["RABBITMQ_HOSTNAME"],
        exchange_name=config["STATIONS_EXCHANGE_NAME"],
        queue_name=config['STATIONS_QUEUE_NAME']
    )
    weather_exchange_writer = ExchangeWriter(
        hostname=config["RABBITMQ_HOSTNAME"],
        exchange_name=config['UNFILTERED_WEATHER_EXCHANGE_NAME'],
        queue_name=config['UNFILTERED_WEATHER_QUEUE_NAME']
    )
    trips_exchange_writer = ExchangeWriter(
        hostname=config["RABBITMQ_HOSTNAME"],
        exchange_name=config['TRIPS_EXCHANGE_NAME'],
        queue_name=config['TRIPS_QUEUE_NAME'],
        exchange_type='fanout')

    new_clients_exchange_writer = ExchangeWriter(
        hostname=config["RABBITMQ_HOSTNAME"],
        exchange_name=config['NEW_CLIENT_EXCHANGE_NAME'],
        queue_name=config['NEW_CLIENT_QUEUE_NAME'],
        exchange_type='fanout')

    server = DataIngestionServer(
        int(config["PORT"]),
        int(config["LISTEN_BACKLOG"]),
        stations_exchange_writer,
        weather_exchange_writer,
        int(config['N_WEATHER_FILTERS']),
        trips_exchange_writer,
        new_clients_exchange_writer,
        config['MONTREAL_STATIONS_OVER_6KM_AVG_TRIP_DISTANCE_RPC_QUEUE_NAME'],
        config['WITH_PRECIPITATIONS_AVG_TRIP_DURATION_RPC_QUEUE_NAME'],
        config['DOUBLED_YEARLY_TRIPS_STATIONS_RPC_QUEUE_NAME']
    )
    server.run()


if __name__ == "__main__":
    main()
