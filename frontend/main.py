from configparser import ConfigParser
from batch_reader.batch_reader import BatchReader
from client import Client
import batch_reader.parse_raw_line
import os


CITIES = ["montreal", "toronto", "washington"]


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def load_sources(config):
    stations_sources = {}
    weather_sources = {}
    trips_sources = {}
    for city in CITIES:
        stations_source = BatchReader(
            config["DATA_PATH"] + "/" + city + "/" + "stations.csv",
            int(config["STATIONS_BATCH_SIZE"]),
            (lambda line: True),
            batch_reader.parse_raw_line.parse_to_station
        )

        weather_source = BatchReader(
            config["DATA_PATH"] + "/" + city + "/" + "weather.csv",
            int(config["WEATHER_BATCH_SIZE"]),
            (lambda line: True),
            batch_reader.parse_raw_line.parse_to_weather
        )

        trips_source = BatchReader(
            config["DATA_PATH"] + "/" + city + "/" + "trips.csv",
            int(config["TRIPS_BATCH_SIZE"]),
            batch_reader.parse_raw_line.validate_trip,
            batch_reader.parse_raw_line.parse_to_trip
        )
        stations_sources[city] = stations_source
        weather_sources[city] = weather_source
        trips_sources[city] = trips_source

    return stations_sources, weather_sources, trips_sources


def main():
    config = read_config()

    stations_sources, weather_sources, trips_sources = load_sources(config)

    client = Client(stations_sources, weather_sources, trips_sources, config)
    client.run()


if __name__ == "__main__":
    main()
