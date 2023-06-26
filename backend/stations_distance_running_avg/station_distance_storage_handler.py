import pickle

from common.processing_node.storage_handler import StorageHandler


class StationDistanceStorageHandler(StorageHandler):

    def _generate_log_map(self, message: bytes):
        joined_trips_with_distance = pickle.loads(message)
        to_log = {}
        for end_station_name, distance in joined_trips_with_distance:
            if end_station_name in to_log:
                to_log[end_station_name]["total_distance"] += distance
                to_log[end_station_name]["n_trips"] += 1
            else:
                last_value = self.storage.get(end_station_name, {"total_distance": 0, "n_trips": 0})
                to_log[end_station_name] = {
                    "total_distance": last_value["total_distance"] + distance,
                    "n_trips": last_value["n_trips"] + 1
                }
        return to_log

    def _update_memory_map_with_logs(self, log_map):
        for end_station_name in log_map:
            new_distance = log_map[end_station_name]["total_distance"]
            new_n_trips = log_map[end_station_name]["n_trips"]
            if end_station_name in self.storage:
                self.storage[end_station_name]["total_distance"] = new_distance
                self.storage[end_station_name]["n_trips"] = new_n_trips
            else:
                self.storage[end_station_name] = {
                    "total_distance": new_distance,
                    "n_trips": new_n_trips
                }
