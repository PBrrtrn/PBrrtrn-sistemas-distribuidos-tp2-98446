import pickle
import common.network.constants
import common.network.deserialize


class FilterProcessInput:
    def __init__(self, filter_function):
        self.filter_function = filter_function

    def filter_process_input(self, message_type: bytes, message_body: bytes):
        if message_type == common.network.constants.TRIPS_BATCH:
            raw_batch, city = pickle.loads(message_body)
            batch = common.network.deserialize.deserialize_trips_batch(raw_batch)
            filtered_trips = self.filter_function(batch)
            serialized_message = pickle.dumps((filtered_trips, city))
            return common.network.constants.TRIPS_BATCH + serialized_message
        else:
            print(f"here{message_type}")
            return None
