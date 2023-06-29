from common.network.socket_wrapper import SocketWrapper
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.fanout_exchange_writer import FanoutExchangeWriter
from common.rabbitmq.rpc_client import RPCClient
import common.network.constants
import common.network.utils
import common.network.deserialize
import pickle


class ClientDataIngestor:
    def __init__(self, wrapped_socket: SocketWrapper,
                 client_id: str,
                 stations_exchange_writer: ExchangeWriter,
                 weather_exchange_writer: ExchangeWriter,
                 n_weather_filters: int,
                 trips_exchange_writer: FanoutExchangeWriter,
                 new_clients_exchange_writer: ExchangeWriter,
                 montreal_stations_over_6km_avg_trip_distance_rpc: RPCClient,
                 with_precipitations_avg_trip_duration_rpc: RPCClient,
                 doubled_yearly_trips_stations_rpc: RPCClient
                 ):
        self.wrapped_socket = wrapped_socket
        self.client_id = client_id
        self.client_id_in_bytes = client_id.encode('utf-8')
        print(f"New client id: {self.client_id_in_bytes}")
        self.stations_exchange_writer = stations_exchange_writer
        self.weather_exchange_writer = weather_exchange_writer
        self.n_weather_filters = n_weather_filters
        self.trips_exchange_writer = trips_exchange_writer
        self.new_clients_exchange_writer = new_clients_exchange_writer
        self.montreal_stations_over_6km_avg_trip_distance_rpc = montreal_stations_over_6km_avg_trip_distance_rpc
        self.with_precipitations_avg_trip_duration_rpc = with_precipitations_avg_trip_duration_rpc
        self.doubled_yearly_trips_stations_rpc = doubled_yearly_trips_stations_rpc
        self.finished = False

    def run(self):
        self.set_up()
        while not self.finished:
            message_type = self.wrapped_socket.recv(common.network.constants.HEADER_TYPE_LEN)
            if message_type == common.network.constants.STATIONS_START:
                self.receive_and_handle_stations_batch(self.wrapped_socket)
            elif message_type == common.network.constants.STATIONS_END_ALL:
                pass
            elif message_type == common.network.constants.WEATHER_START:
                self.receive_and_handle_weather_batch(self.wrapped_socket)
            elif message_type == common.network.constants.WEATHER_END_ALL:
                self.weather_exchange_writer.write(common.network.constants.WEATHER_END_ALL + self.client_id_in_bytes)
            elif message_type == common.network.constants.TRIPS_START:
                self.receive_and_handle_trips_batch(self.wrapped_socket)
            elif message_type == common.network.constants.TRIPS_END_ALL:
                self.notify_trips_end_all()
            elif message_type == common.network.constants.EXECUTE_QUERIES:
                self.execute_queries(self.wrapped_socket)
                self.finished = True
            else:
                print(f"ERROR - Received unexpected message type: {message_type}")

        self.wrapped_socket.close()

    def receive_and_handle_stations_batch(self, wrapped_socket):
        city = common.network.utils.receive_string(wrapped_socket)

        while True:
            message_type = wrapped_socket.recv(common.network.constants.HEADER_TYPE_LEN)
            if message_type == common.network.constants.STATIONS_END:
                self.stations_exchange_writer.write(common.network.constants.STATIONS_END + self.client_id_in_bytes + city.encode('utf-8'),
                                                    routing_key_suffix=self.client_id)
                break
            elif message_type != common.network.constants.STATIONS_BATCH:
                print(
                    f"ERROR - Protocol error (expected {common.network.constants.STATIONS_BATCH}, got {message_type})")
                break

            batch_size = common.network.utils.receive_int(wrapped_socket)
            raw_batch = wrapped_socket.recv(batch_size)

            message = common.network.constants.STATIONS_BATCH + self.client_id_in_bytes + pickle.dumps((raw_batch, city))
            self.stations_exchange_writer.write(message, routing_key_suffix=self.client_id)

    def receive_and_handle_weather_batch(self, wrapped_socket):
        city = common.network.utils.receive_string(wrapped_socket)

        while True:
            message_type = wrapped_socket.recv(common.network.constants.HEADER_TYPE_LEN)
            if message_type == common.network.constants.WEATHER_END:
                break
            elif message_type != common.network.constants.WEATHER_BATCH:
                print(f"ERROR - Protocol error (expected {common.network.constants.WEATHER_BATCH}, got {message_type})")
                break

            batch_size = common.network.utils.receive_int(wrapped_socket)
            raw_batch = wrapped_socket.recv(batch_size)

            message = common.network.constants.WEATHER_BATCH + self.client_id_in_bytes + pickle.dumps((raw_batch, city))
            self.weather_exchange_writer.write(message)

    def receive_and_handle_trips_batch(self, wrapped_socket):
        city = common.network.utils.receive_string(wrapped_socket)

        while True:
            message_type = wrapped_socket.recv(common.network.constants.HEADER_TYPE_LEN)
            if message_type == common.network.constants.TRIPS_END:
                self.trips_exchange_writer.write(common.network.constants.TRIPS_END + self.client_id_in_bytes)
                break
            elif message_type != common.network.constants.TRIPS_BATCH:
                print(f"ERROR - Protocol error (expected {common.network.constants.TRIPS_BATCH}, got {message_type})")
                break

            batch_size = common.network.utils.receive_int(wrapped_socket)
            raw_batch = wrapped_socket.recv(batch_size)

            message = common.network.constants.TRIPS_BATCH + self.client_id_in_bytes + pickle.dumps((raw_batch, city))
            self.trips_exchange_writer.write(message)

    def notify_weather_end_all(self):
        for _ in range(self.n_weather_filters):
            self.weather_exchange_writer.write(common.network.constants.WEATHER_END_ALL + self.client_id_in_bytes)

    def notify_trips_end_all(self):
        self.trips_exchange_writer.write(common.network.constants.TRIPS_END_ALL + self.client_id_in_bytes)

    def execute_queries(self, wrapped_socket):

        response = self.montreal_stations_over_6km_avg_trip_distance_rpc.call(
            common.network.constants.EXECUTE_QUERIES + self.client_id_in_bytes
        )

        wrapped_socket.send(common.network.constants.MONTREAL_STATIONS_OVER_6KM_AVG_TRIP_DISTANCE_RESULT +
                            self.client_id_in_bytes + len(response[4:]).to_bytes(4, 'big') +
                            response[4:])

        raw_avg_duration_response = self.with_precipitations_avg_trip_duration_rpc.call(
            common.network.constants.EXECUTE_QUERIES + self.client_id_in_bytes
        )

        wrapped_socket.send(common.network.constants.WITH_PRECIPITATIONS_AVG_TRIP_DURATION_RESULT +
                            self.client_id_in_bytes + len(raw_avg_duration_response[4:]).to_bytes(4, 'big') +
                            raw_avg_duration_response[4:])

        raw_doubled_station_names_response = self.doubled_yearly_trips_stations_rpc.call(
            common.network.constants.EXECUTE_QUERIES + self.client_id_in_bytes
        )

        wrapped_socket.send(common.network.constants.DOUBLED_YEARLY_TRIPS_STATION_NAMES_RESULT +
                            self.client_id_in_bytes + len(raw_doubled_station_names_response[4:]).to_bytes(4, 'big') +
                            raw_doubled_station_names_response[4:])

        self.montreal_stations_over_6km_avg_trip_distance_rpc.write_eof(common.network.constants.END_QUERY + self.client_id_in_bytes)
        self.with_precipitations_avg_trip_duration_rpc.write_eof(common.network.constants.END_QUERY + self.client_id_in_bytes)
        self.doubled_yearly_trips_stations_rpc.write_eof(common.network.constants.END_QUERY + self.client_id_in_bytes)


    def set_up(self):
        self.new_clients_exchange_writer.write(pickle.dumps(self.client_id))
