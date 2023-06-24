import json
import pickle

from common.processing_node.storage_handler import StorageHandler
import common.network.deserialize


class WeatherStorageHandler(StorageHandler):
    def __init__(self):
        super().__init__()

    def _generate_log_map(self, message: bytes):
        weather_batch, city = pickle.loads(message)
        if city not in self.storage:
            self.storage[city] = {}

        for weather in weather_batch:
            self.storage[city][weather.date] = weather

    def _update_memory_map_with_logs(self, log_map):
        pass

    def __update_changes_in_disk(self):
        pass

