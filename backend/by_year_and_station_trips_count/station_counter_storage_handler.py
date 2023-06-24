import json
import pickle

from common.processing_node.storage_handler import StorageHandler


class StationCounterStorageHandler(StorageHandler):
    def __init__(self):
        super().__init__()

    def _generate_log_map(self, message: bytes):
        trips, city = pickle.loads(message)
        to_log = {}
        for trip in trips:
            if city not in to_log:
                to_log[city] = {}
            trip_year = trip.start_date[:4]
            if trip.start_station_code not in to_log[city]:
                old_value_2016, old_value_2017 = \
                    self.__obtain_old_value_of_station_counter_in_years(
                        city,
                        trip.start_station_code)
                to_log[city][trip.start_station_code] = {
                    '2016': old_value_2016,
                    '2017': old_value_2017
                }
            to_log[city][trip.start_station_code][trip_year] += 1
        return to_log

    def __obtain_old_value_of_station_counter_in_years(self, city, start_station_code):
        if city not in self.storage:
            return 0, 0
        elif start_station_code not in self.storage[city]:
            return 0, 0
        else:
            old_value_2016 = self.storage[city][start_station_code]['2016']
            old_value_2017 = self.storage[city][start_station_code]['2017']
            return old_value_2016, old_value_2017

    def __update_changes_in_disk(self):
        pass

    def _update_memory_map_with_logs(self, log_map):
        for city in log_map:
            if city not in self.storage:
                self.storage[city] = {}
            for start_station_code in log_map[city]:
                if start_station_code not in self.storage[city]:
                    self.storage[city][start_station_code] = {'2016': 0, '2017': 0}
                for year in log_map[city][start_station_code]:
                    new_value = log_map[city][start_station_code][year]
                    self.storage[city][start_station_code][year] = new_value

