import struct
from common.network import constants


def serialize_stations_start(city: str):
    return constants.STATIONS_START + len(city).to_bytes(4, 'big') + city.encode('utf-8')


def serialize_stations_batch(batch):
    serialized_stations = b''
    for station in batch:
        serialized_stations += serialize_station(station)

    return constants.STATIONS_BATCH + len(serialized_stations).to_bytes(4, 'big') + serialized_stations


def serialize_stations_end():
    return constants.STATIONS_END


def serialize_stations_end_all():
    return constants.STATIONS_END_ALL


def serialize_station(station):
    serialized_station = b''

    serialized_station += station.code.to_bytes(4, 'big')

    serialized_station_name = station.name.encode('utf-8')
    serialized_station += len(serialized_station_name).to_bytes(4, 'big')
    serialized_station += serialized_station_name

    serialized_station += struct.pack('>f', station.latitude)
    serialized_station += struct.pack('>f', station.longitude)

    return serialized_station


def serialize_weather_start(city):
    return constants.WEATHER_START + len(city).to_bytes(4, 'big') + city.encode('utf-8')


def serialize_weather_batch(batch):
    serialized_weathers = b''

    for weather in batch:
        serialized_weathers += serialize_weather(weather)

    return constants.WEATHER_BATCH + len(serialized_weathers).to_bytes(4, 'big') + serialized_weathers


def serialize_weather(weather):
    serialized_weather = b''

    serialized_weather += weather.date.encode('utf-8')  # 10 chars
    serialized_weather += struct.pack('>f', weather.precipitations)

    return serialized_weather


def serialize_weather_end():
    return constants.WEATHER_END


def serialize_weather_end_all():
    return constants.WEATHER_END_ALL


def serialize_trips_start(city):
    return constants.TRIPS_START + len(city).to_bytes(4, 'big') + city.encode('utf-8')


def serialize_trips_batch(batch):
    serialized_trips = b''
    for trip in batch:
        serialized_trips += serialize_trip(trip)

    return constants.TRIPS_BATCH + len(serialized_trips).to_bytes(4, 'big') + serialized_trips


def serialize_trip(trip):
    serialized_trip = b''

    serialized_trip += trip.start_date.encode('utf-8')  # 19 chars
    serialized_trip += trip.start_station_code.to_bytes(4, 'big')
    serialized_trip += trip.end_date.encode('utf-8')  # 19 chars
    serialized_trip += trip.end_station_code.to_bytes(4, 'big')
    serialized_trip += trip.duration_sec.to_bytes(4, 'big')
    serialized_trip += b'1' if trip.is_member else b'0'
    serialized_trip += trip.year_id.to_bytes(4, 'big')

    return serialized_trip


def serialize_trips_end():
    return constants.TRIPS_END


def serialize_trips_end_all():
    return constants.TRIPS_END_ALL


def serialize_queries_request():
    return constants.EXECUTE_QUERIES
