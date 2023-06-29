import pickle
import common.network.constants


class RPCStationInputProcessor:
    FILTER_DISTANCE = 6.0

    def __init__(self):
        self.storage = None

    def set_storage(self, storage):
        self.storage = storage

    def process_input(self, message_type: bytes, message_body: bytes, client_id, message_id):
        header = message_type + client_id.encode() + message_id.encode()
        if message_type == common.network.constants.TRIPS_BATCH:
            return header + self.join_trips(message_body)
        elif message_type == common.network.constants.STATIONS_BATCH:
            return header + self.join_stations(message_body)
        else:
            print(f"ERROR - Unknown message header (got {message_type})")

    def join_stations(self, raw_message):
        station_codes, city = pickle.loads(raw_message)
        response = []
        for station_code in station_codes:
            station = self.storage[city][station_code]
            response.append(station.name)
        return pickle.dumps(response)

    def join_trips(self, raw_message):
        raw_batch, city = pickle.loads(raw_message)
        trips_batch = common.network.deserialize.deserialize_trips_batch(raw_batch)
        response = []
        for trip in trips_batch:
            if trip.start_station_code in self.storage[city] and trip.end_station_code in self.storage[city]:
                start_station = self.storage[city][trip.start_station_code]
                end_station = self.storage[city][trip.end_station_code]
                response.append((trip, start_station, end_station))
        return pickle.dumps(response)
