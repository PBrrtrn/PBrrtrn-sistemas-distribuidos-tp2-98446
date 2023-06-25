import pickle

from common.processing_node.storage_handler import StorageHandler
import common.network.deserialize


class WeatherStorageHandler(StorageHandler):
    def __init__(self):
        super().__init__()

    def _generate_log_map(self, message: bytes):
        weather_batch, city = pickle.loads(message)
        to_log = {city: {}}
        for weather in weather_batch:
            to_log[city][weather.date] = weather
        return to_log

    def _update_memory_map_with_logs(self, log_map):
        for city in log_map:
            if city not in self.storage:
                self.storage[city] = {}
            for date in log_map[city]:
                self.storage[city][date] = log_map[city][date]
