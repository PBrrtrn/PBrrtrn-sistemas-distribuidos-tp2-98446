from multiprocessing import Process
from typing import Callable

from common.network.socket_wrapper import SocketWrapper

import socket

import common.network.constants

from client_data_ingestor import ClientDataIngestor


class DataIngestionServer:
    def __init__(self, port,
                 listen_backlog,
                 exchange_writers_factory: Callable,
                 rpc_clients_factory: Callable,
                 n_weather_filters: int,
                 config):
        self.port = port
        self.listen_backlog = listen_backlog
        self.exchange_writers_factory = exchange_writers_factory
        self.rpc_clients_factory = rpc_clients_factory
        self.n_weather_filters = n_weather_filters
        self.finished = False
        self.config = config
        self.clients_id = []

    def run(self):
        server_socket = socket.socket()
        server_socket.bind(('', self.port))
        server_socket.listen(self.listen_backlog)

        while True:
            client_socket, _ = server_socket.accept()
            wrapped_socket = SocketWrapper(client_socket)
            client_id = str(len(self.clients_id) + 1).zfill(common.network.constants.CLIENT_ID_LEN)
            stations_exchange_writer, weather_exchange_writer, trips_exchange_writer, \
            new_clients_exchange_writer = self.exchange_writers_factory(self.config)
            montreal_stations_over_6km_avg_trip_distance_rpc, with_precipitations_avg_trip_duration_rpc, \
            doubled_yearly_trips_stations_rpc = self.rpc_clients_factory(self.config, client_id)

            client_ingestor = ClientDataIngestor(wrapped_socket, client_id, stations_exchange_writer,
                                                 weather_exchange_writer, self.n_weather_filters,
                                                 trips_exchange_writer,
                                                 new_clients_exchange_writer,
                                                 montreal_stations_over_6km_avg_trip_distance_rpc,
                                                 with_precipitations_avg_trip_duration_rpc,
                                                 doubled_yearly_trips_stations_rpc)
            client = Process(target=client_ingestor.run, args=(), daemon=True)
            client.start()
            self.clients_id.append(client_id)
