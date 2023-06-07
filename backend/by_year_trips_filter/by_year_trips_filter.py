import pickle

import common.network.constants
import common.network.deserialize

from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.exchange_writer import ExchangeWriter


class ByYearTripsFilter:
    def __init__(self, trips_input_queue_reader: QueueReader, filtered_trips_exchange_writer: ExchangeWriter):
        self.trips_input_queue_reader = trips_input_queue_reader
        self.filtered_trips_exchange_writer = filtered_trips_exchange_writer # TODO: Revisar que esté bien

    def run(self):
        self.trips_input_queue_reader.consume(self.filter_trips_batch)

    def filter_trips_batch(self, _channel, _method, _properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.TRIPS_BATCH:
            raw_message = body[common.network.constants.HEADER_TYPE_LEN:]
            raw_batch, city = pickle.loads(raw_message)
            batch = common.network.deserialize.deserialize_trips_batch(raw_batch)

            filtered_trips = []
            for trip in batch:
                if trip.start_date[:4] == "2017" or trip.start_date[:4] == "2016":
                    filtered_trips.append(trip)

            serialized_message = pickle.dumps((filtered_trips, city))
            self.filtered_trips_exchange_writer.write(common.network.constants.TRIPS_BATCH + serialized_message)
        elif message_type == common.network.constants.TRIPS_END_ALL:
            self.trips_input_queue_reader.shutdown()
            self.filtered_trips_exchange_writer.write(common.network.constants.TRIPS_END_ALL)
