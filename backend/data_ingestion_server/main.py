import os
from configparser import ConfigParser

from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.fanout_exchange_writer import FanoutExchangeWriter
from common.rabbitmq.rpc_client import RPCClient
from data_ingestion_server import DataIngestionServer


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def exchange_writers_factory(config):
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
    trips_exchange_writer = FanoutExchangeWriter(
        hostname=config["RABBITMQ_HOSTNAME"],
        exchange_name=config['TRIPS_EXCHANGE_NAME'])

    new_clients_exchange_writer = ExchangeWriter(
        hostname=config["RABBITMQ_HOSTNAME"],
        exchange_name=config['NEW_CLIENT_EXCHANGE_NAME'],
        queue_name=config['NEW_CLIENT_QUEUE_NAME'],
        exchange_type='fanout')

    return stations_exchange_writer, weather_exchange_writer, trips_exchange_writer, new_clients_exchange_writer


def rpc_clients_factory(config, client_id):
    montreal_stations_over_6km_avg_trip_distance_rpc = RPCClient(
        f"{config['MONTREAL_STATIONS_OVER_6KM_AVG_TRIP_DISTANCE_RPC_QUEUE_NAME']}{client_id}"
    )
    with_precipitations_avg_trip_duration_rpc = RPCClient(
        f"{config['WITH_PRECIPITATIONS_AVG_TRIP_DURATION_RPC_QUEUE_NAME']}{client_id}"
    )
    doubled_yearly_trips_stations_rpc = RPCClient(
        f"{config['DOUBLED_YEARLY_TRIPS_STATIONS_RPC_QUEUE_NAME']}{client_id}"
    )

    return montreal_stations_over_6km_avg_trip_distance_rpc, with_precipitations_avg_trip_duration_rpc, \
           doubled_yearly_trips_stations_rpc


def main():
    config = read_config()

    server = DataIngestionServer(
        int(config["PORT"]),
        int(config["LISTEN_BACKLOG"]),
        exchange_writers_factory,
        rpc_clients_factory,
        int(config['N_WEATHER_FILTERS']),
        config
    )
    server.run()


if __name__ == "__main__":
    main()
