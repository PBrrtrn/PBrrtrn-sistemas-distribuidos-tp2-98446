import pickle
import common.network.constants


class RPCDurationInputProcessor:
    FILTER_DISTANCE = 6.0

    def __init__(self):
        self.storage = None

    def set_storage(self, storage):
        self.storage = storage

    def process_input(self, message_type: bytes, _message_body: bytes):
        if message_type == common.network.constants.EXECUTE_QUERIES:
            response = self.storage['total_duration'] / self.storage['n_trips']
            return pickle.dumps(response)
