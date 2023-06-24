import json
import pickle

from common.processing_node.storage_handler import StorageHandler

LOG_FILE_PATH = "path"
STORAGE_FILE_PATH = "storage_path"
AUX_STORAGE_FILE_PATH = "storage_path"
COMMIT_CHAR = "~"


class StationCounterStorageHandler(StorageHandler):
    def __init__(self):
        super(StorageHandler, self).__init__()
        self.storage = {}

    def prepare(self, message: bytes):
        trips, city = pickle.loads(message)
        to_log = {}
        for trip in trips:
            if city not in to_log:
                to_log[city] = {}
            trip_year = trip.start_date[:4]
            if trip.start_station_code not in to_log[city]:
                old_value_2016 = \
                    self.__obtain_old_value_of_station_counter_in_year(
                        '2016',
                        city,
                        trip.start_station_code)
                old_value_2017 = \
                    self.__obtain_old_value_of_station_counter_in_year(
                        '2017',
                        city,
                        trip.start_station_code)
                to_log[city][trip.start_station_code] = {'2016': old_value_2016, '2017': old_value_2017}
            to_log[city][trip.start_station_code][trip_year] += 1

        # update memory map
        # flush log to file (operations can be interchangeably done, does not matter the order)
        self.__update_memory_map_with_logs(to_log)
        self.__write_log_line(to_log)
        """
        trips, city = pickle.loads(message)
        for trip in trips:
            if city not in self.storage:
                self.storage[city] = {}

            if trip.start_station_code not in self.storage[city]:
                self.storage[city][trip.start_station_code] = {'2016': 0, '2017': 0}

            trip_year = trip.start_date[:4]
            self.storage[city][trip.start_station_code][trip_year] += 1
"""

    def __obtain_old_value_of_station_counter_in_year(self, year, city, start_station_code):
        if city not in self.storage:
            return 0
        elif start_station_code not in self.storage[city]:
            return 0
        else:
            return self.storage[city][start_station_code][year]

    def __write_log_line(self, to_log):
        pass
        # with open(LOG_FILE_PATH, "w") as file:
        #   json.dump(to_log, file) #Archivo ya abierto + flush

    def commit(self):
        pass

    def __update_changes_in_disk(self):
        pass

    def __update_memory_map_with_logs(self, log_map):
        for city in log_map:
            if city not in self.storage:
                self.storage[city] = {}
            for start_station_code in log_map[city]:
                if start_station_code not in self.storage[city]:
                    self.storage[city][start_station_code] = {'2016': 0, '2017': 0}
                for year in log_map[city][start_station_code]:
                    new_value = log_map[city][start_station_code][year]
                    self.storage[city][start_station_code][year] = new_value

    def get_storage(self):
        return self.storage

    def prepare_delete(self):
        pass

    def commit_delete(self):
        pass
