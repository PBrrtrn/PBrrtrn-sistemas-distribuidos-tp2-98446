import common.network.constants
import common.network.deserialize
import pickle


def by_year_trips_filter_process_input(message_type: bytes, message_body: bytes):
    if message_type == common.network.constants.TRIPS_BATCH:
        raw_batch, city = pickle.loads(message_body)
        batch = common.network.deserialize.deserialize_trips_batch(raw_batch)

        filtered_trips = []
        for trip in batch:
            if trip.start_date[:4] == "2017" or trip.start_date[:4] == "2016":
                filtered_trips.append(trip)
        if len(filtered_trips) == 0:
            return None
        else:
            return common.network.constants.TRIPS_BATCH + pickle.dumps((filtered_trips, city))
