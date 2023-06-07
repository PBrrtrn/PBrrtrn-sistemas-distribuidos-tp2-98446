import struct
from typing import Callable

from common.model.station import Station
from common.model.weather import Weather
from common.model.trip import Trip


def deserialize_stations_batch(raw_batch: bytes):
    return deserialize_batch(raw_batch, deserialize_station)


def deserialize_station(station_bytes: bytes):
    code = int.from_bytes(station_bytes[0:4], 'big')

    name_len = int.from_bytes(station_bytes[4:8], 'big')
    name = station_bytes[8:8+name_len].decode('utf-8')

    latitude = struct.unpack('>f', station_bytes[8+name_len:12+name_len])[0]
    longitude = struct.unpack('>f', station_bytes[12+name_len:16+name_len])[0]

    return Station(code, name, float(latitude), float(longitude)), 16 + name_len


def deserialize_weather_batch(raw_batch: bytes):
    return deserialize_batch(raw_batch, deserialize_weather)


def deserialize_weather(weather_bytes: bytes):
    date = weather_bytes[0:10].decode('utf-8')
    precipitations = struct.unpack('>f', weather_bytes[10:14])[0]

    return Weather(date, precipitations), 14


def deserialize_trips_batch(raw_batch: bytes):
    return deserialize_batch(raw_batch, deserialize_trip)


def deserialize_trip(trip_bytes: bytes):
    start_date = trip_bytes[:19].decode('utf-8')
    start_station_code = int.from_bytes(trip_bytes[19:23], 'big')
    end_date = trip_bytes[23:42]
    end_station_code = int.from_bytes(trip_bytes[42:46], 'big')
    duration = int.from_bytes(trip_bytes[46:50], 'big')
    is_member = bool.from_bytes(trip_bytes[50:51], 'big')
    year_id = int.from_bytes(trip_bytes[51:55], 'big')

    trip = Trip(start_date, start_station_code, end_date, end_station_code, duration, is_member, year_id)
    return trip, 55


def deserialize_batch(raw_batch: bytes, deserialize_item: Callable):
    batch = []
    total_read_bytes = 0
    while total_read_bytes < len(raw_batch):
        item, read_bytes = deserialize_item(raw_batch[total_read_bytes:])
        batch.append(item)
        total_read_bytes += read_bytes

    return batch

