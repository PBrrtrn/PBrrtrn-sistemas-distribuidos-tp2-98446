import pickle

from common.processing_node.queue_consumer.output_processor.storage_handler import StorageHandler


class StationCounterStorageHandler(StorageHandler):
    def _generate_log_map(self, message: bytes):
        trips, city = pickle.loads(message)
        to_log = {}
        for trip in trips:
            if city not in to_log:
                to_log[city] = {}
            trip_year = trip.start_date[:4]
            if trip.start_station_code not in to_log[city]:
                last_value_2016, last_value_2017 = \
                    self.__obtain_last_value_of_station_counter_in_years(
                        city,
                        trip.start_station_code)
                to_log[city][trip.start_station_code] = {
                    '2016': last_value_2016,
                    '2017': last_value_2017
                }
            to_log[city][trip.start_station_code][trip_year] += 1
        return to_log

    def __obtain_last_value_of_station_counter_in_years(self, city, start_station_code):
        if city not in self.storage:
            return 0, 0
        elif start_station_code not in self.storage[city]:
            return 0, 0
        else:
            last_value_2016 = self.storage[city][start_station_code]['2016']
            last_value_2017 = self.storage[city][start_station_code]['2017']
            return last_value_2016, last_value_2017
            # self.storage.fetch(city, {}).fetch(start_station_code, {'2016': 0, '2017': 0})
            # return self.storage[city][start_station_code]

    def __update_changes_in_disk(self):
        pass

    def _update_memory_map_with_logs(self, storage, log_entry):
        for city in log_entry:
            if city not in storage:
                storage[city] = {}
            for start_station_code in log_entry[city]:
                if start_station_code not in storage[city]:
                    storage[city][start_station_code] = {'2016': 0, '2017': 0}
                for year in log_entry[city][start_station_code]:
                    new_value = log_entry[city][start_station_code][year]
                    storage[city][start_station_code][year] = new_value
