import pickle
import common.network.constants
import common.network.deserialize


class RPCWeatherInputProcessor:
    TRIP_DATE_LEN = 10

    def __init__(self):
        self.storage = None

    def set_storage(self, storage):
        self.storage = storage

    def process_input(self, message_type: bytes, message_body: bytes, client_id, message_id):
        if message_type == common.network.constants.TRIPS_BATCH:
            raw_batch, city = pickle.loads(message_body)
            trips_batch = common.network.deserialize.deserialize_trips_batch(raw_batch)
            response = []
            if city in self.storage:
                for trip in trips_batch:
                    trip_date = trip.start_date[:self.TRIP_DATE_LEN]
                    if trip_date in self.storage[city]:
                        response.append(trip)

            return message_type + client_id.encode() + message_id.encode() + pickle.dumps(response)
        else:
            print(f"ERROR - Unknown message header (got {message_type})")
