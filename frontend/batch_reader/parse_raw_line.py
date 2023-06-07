from common.model.station import Station
from common.model.weather import Weather
from common.model.trip import Trip


def parse_to_station(raw_line):
    latitude = 0.0 if len(raw_line[2]) == 0 else float(raw_line[2])
    longitude = 0.0 if len(raw_line[3]) == 0 else float(raw_line[3])
    return Station(int(raw_line[0]), raw_line[1], latitude, longitude)


def parse_to_weather(raw_line):
    return Weather(raw_line[0], float(raw_line[1]))


def validate_trip(raw_line):
    return int(float(raw_line[4])) > 0


def parse_to_trip(raw_line):
    return Trip(
        raw_line[0],
        int(raw_line[1]),
        raw_line[2],
        int(raw_line[3]),
        int(float(raw_line[4])),
        bool(raw_line[5]),
        int(raw_line[6])
    )
