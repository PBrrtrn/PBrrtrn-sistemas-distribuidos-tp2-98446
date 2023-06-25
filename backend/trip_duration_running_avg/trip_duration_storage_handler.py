import json
import pickle

from common.processing_node.storage_handler import StorageHandler


class TripDurationStorageHandler(StorageHandler):
    def __init__(self):
        super().__init__()
        self.storage = {'total_duration': 0, 'n_trips': 0}

    def _generate_log_map(self, message: bytes):
        trips = pickle.loads(message)
        for trip in trips:
            self.storage['total_duration'] += trip.duration_sec
            self.storage['n_trips'] += 1

    def _update_memory_map_with_logs(self, log_map):
        pass

    def __update_changes_in_disk(self):
        pass

