import json
import pickle

from common.processing_node.storage_handler import StorageHandler

LOG_FILE_PATH = "path"
STORAGE_FILE_PATH = "storage_path"
AUX_STORAGE_FILE_PATH = "storage_path"
COMMIT_CHAR = "~"


class StationDistanceStorageHandler(StorageHandler):
    def __init__(self):
        super(StorageHandler, self).__init__()
        self.storage = {}

    def prepare(self, message: bytes):
        joined_trips_with_distance = pickle.loads(message)

        for end_station_name, distance in joined_trips_with_distance:
            if end_station_name in self.storage:
                self.storage[end_station_name]["total_distance"] += distance
                self.storage[end_station_name]["n_trips"] += 1
            else:
                self.storage[end_station_name] = {"total_distance": distance, "n_trips": 1}
        #self.__update_memory_map_with_logs(to_log)
        #self.__write_log_line(to_log)

    def __write_log_line(self, to_log):
        pass
        # with open(LOG_FILE_PATH, "w") as file:
        #   json.dump(to_log, file) #Archivo ya abierto + flush

    def commit(self):
        pass

    def __update_changes_in_disk(self):
        pass

    def __update_memory_map_with_logs(self, log_map):
        pass

    def get_storage(self):
        return self.storage

    def prepare_delete(self):
        pass

    def commit_delete(self):
        pass
