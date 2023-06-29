import pickle

import common.network.constants
from haversine import haversine


def distance_calculator_process_input(message_type: bytes, message_body: bytes, client_id):
    if message_type == common.network.constants.TRIPS_BATCH:
        return _calculate_distances(message_body, client_id)
    else:
        print(f"ERROR - Received unknown message type ({message_type})")


def _calculate_distances(raw_batch: bytes, client_id):
    joined_trips = pickle.loads(raw_batch)

    distances_batch = []
    for trip, start_station, end_station in joined_trips:
        start = (start_station.latitude, start_station.longitude)
        end = (end_station.latitude, end_station.longitude)
        distance = haversine(start, end)
        distances_batch.append((end_station.name, distance))

    serialized_batch = common.network.constants.TRIPS_BATCH + client_id.encode() + pickle.dumps(distances_batch)
    return serialized_batch
