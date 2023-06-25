import pickle
import common.network.constants


class RPCDistanceInputProcessor:
    FILTER_DISTANCE = 6.0

    def __init__(self):
        self.storage = None

    def set_storage(self, storage):
        self.storage = storage

    def process_input(self, message_type: bytes, _message_body: bytes):
        if message_type == common.network.constants.EXECUTE_QUERIES:
            response = []
            for station_name, attributes in self.storage.items():
                avg_distance = attributes["total_distance"] / attributes["n_trips"]
                if avg_distance > self.FILTER_DISTANCE:
                    response.append(station_name)
            return pickle.dumps(response)
