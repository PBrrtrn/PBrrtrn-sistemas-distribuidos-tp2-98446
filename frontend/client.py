import pickle
import socket
import common.network.serialize
import common.network.constants
import common.network.utils
from common.network.socket_wrapper import SocketWrapper


class Client:
    def __init__(self, stations_sources, weather_sources, trips_sources, config):
        self.stations_sources = stations_sources
        self.weather_sources = weather_sources
        self.trips_sources = trips_sources
        self.config = config

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.config['SERVER_ADDRESS'], int(self.config["SERVER_PORT"])))
        except TimeoutError:
            # TODO: Log
            return

        wrapped_socket = SocketWrapper(sock)
        self.send_stations(wrapped_socket)
        self.send_weather(wrapped_socket)
        self.send_trips(wrapped_socket)

        send_queries_request(wrapped_socket)

    def send_stations(self, wrapped_socket):
        for city, source in self.stations_sources.items():
            send_stations_start(city, wrapped_socket)
            for batch in source.read():
                send_stations_batch(batch, wrapped_socket)
            send_stations_end(wrapped_socket)
        send_stations_end_all(wrapped_socket)

    def send_weather(self, wrapped_socket):
        for city, source in self.weather_sources.items():
            send_weather_start(city, wrapped_socket)
            for batch in source.read():
                send_weather_batch(batch, wrapped_socket)
            send_weather_end(wrapped_socket)
        send_weather_end_all(wrapped_socket)

    def send_trips(self, wrapped_socket):
        for city, source in self.trips_sources.items():
            send_trips_start(city, wrapped_socket)
            for batch in source.read():
                send_trips_batch(batch, wrapped_socket)
            send_trips_end(wrapped_socket)
        send_trips_end_all(wrapped_socket)


N_QUERIES = 3


def send_stations_batch(batch, wrapped_socket):
    serialized_batch = common.network.serialize.serialize_stations_batch(batch)
    wrapped_socket.send(serialized_batch)


def send_stations_start(city, wrapped_socket):
    serialized_stations_start = common.network.serialize.serialize_stations_start(city)
    wrapped_socket.send(serialized_stations_start)


def send_stations_end(wrapped_socket):
    serialized_stations_end = common.network.serialize.serialize_stations_end()
    wrapped_socket.send(serialized_stations_end)


def send_stations_end_all(wrapped_socket):
    serialized_stations_end_all = common.network.serialize.serialize_stations_end_all()
    wrapped_socket.send(serialized_stations_end_all)


def send_weather_start(city, wrapped_socket):
    serialized_weathers_start = common.network.serialize.serialize_weather_start(city)
    wrapped_socket.send(serialized_weathers_start)


def send_weather_batch(batch, wrapped_socket):
    serialized_batch = common.network.serialize.serialize_weather_batch(batch)
    wrapped_socket.send(serialized_batch)


def send_weather_end(wrapped_socket):
    serialized_weather_end = common.network.serialize.serialize_weather_end()
    wrapped_socket.send(serialized_weather_end)


def send_weather_end_all(wrapped_socket):
    serialized_weather_end_all = common.network.serialize.serialize_weather_end_all()
    wrapped_socket.send(serialized_weather_end_all)


def send_trips_start(city, wrapped_socket):
    serialized_trips_start = common.network.serialize.serialize_trips_start(city)
    wrapped_socket.send(serialized_trips_start)


def send_trips_batch(batch, wrapped_socket):
    serialized_batch = common.network.serialize.serialize_trips_batch(batch)
    wrapped_socket.send(serialized_batch)


def send_trips_end(wrapped_socket):
    serialized_trips_end = common.network.serialize.serialize_trips_end()
    wrapped_socket.send(serialized_trips_end)


def send_trips_end_all(wrapped_socket):
    serialized_trips_end_all = common.network.serialize.serialize_trips_end_all()
    wrapped_socket.send(serialized_trips_end_all)


def send_queries_request(wrapped_socket):
    wrapped_socket.send(common.network.serialize.serialize_queries_request())

    for _ in range(N_QUERIES):
        message_type = wrapped_socket.recv(common.network.constants.HEADER_TYPE_LEN)
        if message_type == common.network.constants.MONTREAL_STATIONS_OVER_6KM_AVG_TRIP_DISTANCE_RESULT:
            message_length = common.network.utils.receive_int(wrapped_socket)
            raw_stations = wrapped_socket.recv(message_length)
            stations = ', '.join(pickle.loads(raw_stations))
            print(f"INFO - Montreal stations with average trip distance over 6km: {stations}")
        elif message_type == common.network.constants.WITH_PRECIPITATIONS_AVG_TRIP_DURATION_RESULT:
            message_length = common.network.utils.receive_int(wrapped_socket)
            raw_avg_duration_with_precipitations = wrapped_socket.recv(message_length)
            avg_duration_precip = round(pickle.loads(raw_avg_duration_with_precipitations), 2)
            print(f"INFO - Average duration for trips with >30mm precipitations: {avg_duration_precip} sec.")
        elif message_type == common.network.constants.DOUBLED_YEARLY_TRIPS_STATION_NAMES_RESULT:
            message_length = common.network.utils.receive_int(wrapped_socket)
            raw_stations = wrapped_socket.recv(message_length)
            stations = ', '.join(pickle.loads(raw_stations))
            print(f"INFO - Stations with doubled yearly trips over 2017 and 2016: {stations}")

    wrapped_socket.close()
