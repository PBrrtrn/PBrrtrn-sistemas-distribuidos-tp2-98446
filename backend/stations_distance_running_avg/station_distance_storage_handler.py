import json
import pickle

from common.processing_node.storage_handler import StorageHandler


class StationDistanceStorageHandler(StorageHandler):
    def __init__(self):
        super().__init__()

    def _generate_log_map(self, message: bytes):
        joined_trips_with_distance = pickle.loads(message)

        for end_station_name, distance in joined_trips_with_distance:
            if end_station_name in self.storage:
                self.storage[end_station_name]["total_distance"] += distance
                self.storage[end_station_name]["n_trips"] += 1
            else:
                self.storage[end_station_name] = {"total_distance": distance, "n_trips": 1}

    def _update_memory_map_with_logs(self, log_map):
        pass

    def __update_changes_in_disk(self):
        pass

