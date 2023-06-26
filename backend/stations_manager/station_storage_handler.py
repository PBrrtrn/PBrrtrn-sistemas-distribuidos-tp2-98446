import pickle

import common.model.station
from common.processing_node.storage_handler import StorageHandler
import common.network.deserialize


class StationStorageHandler(StorageHandler):
    def _generate_log_map(self, message: bytes):
        deserialized_message = pickle.loads(message)
        raw_stations_batch, city = deserialized_message[0], deserialized_message[1]
        stations_batch = common.network.deserialize.deserialize_stations_batch(raw_stations_batch)
        to_log = {city: {}}
        for station in stations_batch:
            to_log[city][station.code] = common.model.station.to_dict(station)
        return to_log

    def _update_memory_map_with_logs(self, log_map):
        for city in log_map:
            if city not in self.storage:
                self.storage[city] = {}
            for station_code in log_map[city]:
                self.storage[city][station_code] = common.model.station.from_dict(log_map[city][station_code])
