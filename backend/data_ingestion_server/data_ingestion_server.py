from multiprocessing import Process

from common.network.socket_wrapper import SocketWrapper


import socket

from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.rpc_client import RPCClient

from client_data_ingestor import ClientDataIngestor

class DataIngestionServer:
    def __init__(self, port,
                 listen_backlog,
                 stations_exchange_writer: ExchangeWriter,
                 weather_exchange_writer: ExchangeWriter,
                 n_weather_filters: int,
                 trips_exchange_writer: ExchangeWriter,
                 new_clients_exchange_writer: ExchangeWriter,
                 montreal_stations_over_6km_avg_trip_distance_queue_name: str,
                 with_precipitations_avg_trip_duration_queue_name: str,
                 doubled_yearly_trips_stations_queue_name: str):
        self.port = port
        self.listen_backlog = listen_backlog
        self.stations_exchange_writer = stations_exchange_writer
        self.weather_exchange_writer = weather_exchange_writer
        self.n_weather_filters = n_weather_filters
        self.trips_exchange_writer = trips_exchange_writer
        self.new_clients_exchange_writer = new_clients_exchange_writer
        self.montreal_stations_over_6km_avg_trip_distance_queue_name =\
            montreal_stations_over_6km_avg_trip_distance_queue_name
        self.with_precipitations_avg_trip_duration_queue_name = with_precipitations_avg_trip_duration_queue_name
        self.doubled_yearly_trips_stations_queue_name = doubled_yearly_trips_stations_queue_name
        self.finished = False
        self.clients_id = []

    def run(self):
        server_socket = socket.socket()
        server_socket.bind(('', self.port))
        server_socket.listen(self.listen_backlog)

        client_socket, _ = server_socket.accept()
        wrapped_socket = SocketWrapper(client_socket)
        client_id = str(len(self.clients_id) + 1)
        montreal_stations_over_6km_avg_trip_distance_rpc = RPCClient(
            f"{self.montreal_stations_over_6km_avg_trip_distance_queue_name}{client_id}"
        )
        with_precipitations_avg_trip_duration_rpc = RPCClient(
            f"{self.with_precipitations_avg_trip_duration_queue_name}{client_id}"
        )
        doubled_yearly_trips_stations_rpc = RPCClient(
            f"{self.doubled_yearly_trips_stations_queue_name}{client_id}"
        )
        client_ingestor = ClientDataIngestor(wrapped_socket, client_id, self.stations_exchange_writer,
                                             self.weather_exchange_writer, self.n_weather_filters, 
                                             self.trips_exchange_writer,
                                             self.new_clients_exchange_writer,
                                             montreal_stations_over_6km_avg_trip_distance_rpc,
                                             with_precipitations_avg_trip_duration_rpc,
                                             doubled_yearly_trips_stations_rpc)
        client = Process(target=client_ingestor.run, args=(), daemon=True)
        client.start()
        self.clients_id.append(client_id)
        client.join()
