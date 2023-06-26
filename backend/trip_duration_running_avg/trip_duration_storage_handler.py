import pickle

from common.processing_node.storage_handler import StorageHandler


class TripDurationStorageHandler(StorageHandler):

    def _generate_log_map(self, message: bytes):
        trips = pickle.loads(message)
        duration_sec_counter = 0
        n_trips_counter = 0
        for trip in trips:
            duration_sec_counter += trip.duration_sec
            n_trips_counter += 1
        return {
            'total_duration': self.storage.get('total_duration', 0) + duration_sec_counter,
            'n_trips': self.storage.get('n_trips', 0) + n_trips_counter
        }

    def _update_memory_map_with_logs(self, log_map):
        new_total_duration = log_map['total_duration']
        new_total_n_trips = log_map['n_trips']
        if len(self.storage) == 0:
            self.storage = {'total_duration': new_total_duration, 'n_trips': new_total_n_trips}
        else:
            self.storage['total_duration'] = new_total_duration
            self.storage['n_trips'] = new_total_n_trips
