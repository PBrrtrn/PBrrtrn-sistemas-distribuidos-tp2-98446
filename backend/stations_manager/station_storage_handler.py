import json
import pickle

from common.processing_node.storage_handler import StorageHandler
import common.network.deserialize


class StationStorageHandler(StorageHandler):
    def __init__(self):
        super().__init__()

    def _generate_log_map(self, message: bytes):
        deserialized_message = pickle.loads(message)

        raw_stations_batch, city = deserialized_message[0], deserialized_message[1]
        stations_batch = common.network.deserialize.deserialize_stations_batch(raw_stations_batch)

        if city not in self.storage:
            self.storage[city] = {}

        for station in stations_batch:
            self.storage[city][station.code] = station

    def _update_memory_map_with_logs(self, log_map):
        pass

    def __update_changes_in_disk(self):
        pass

