import pickle

import common.network.constants
from haversine import haversine


def distance_calculator_process_input(message_type: bytes, message_body: bytes, client_id, message_id):
    if message_type == common.network.constants.TRIPS_BATCH:
        return common.network.constants.TRIPS_BATCH + client_id.encode() + message_id.encode() + \
               _calculate_distances(message_body)
    else:
        print(f"ERROR - Received unknown message type ({message_type})")


def _calculate_distances(raw_batch: bytes):
    joined_trips = pickle.loads(raw_batch)

    distances_batch = []
    for trip, start_station, end_station in joined_trips:
        start = (start_station.latitude, start_station.longitude)
        end = (end_station.latitude, end_station.longitude)
        distance = haversine(start, end)
        distances_batch.append((end_station.name, distance))

    return pickle.dumps(distances_batch)
