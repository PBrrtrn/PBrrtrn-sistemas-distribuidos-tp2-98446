import pickle

from common.processing_node.storage_handler import StorageHandler
import common.model.weather


class WeatherStorageHandler(StorageHandler):
    def _generate_log_map(self, message: bytes):
        weather_batch, city = pickle.loads(message)
        to_log = {city: {}}
        for weather in weather_batch:
            to_log[city][weather.date] = common.model.weather.to_dict(weather)
        return to_log

    def _update_memory_map_with_logs(self, log_map):
        for city in log_map:
            if city not in self.storage:
                self.storage[city] = {}
            for date in log_map[city]:
                self.storage[city][date] = common.model.weather.from_dict(log_map[city][date])
