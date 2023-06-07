import pickle

import common.network.constants
from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.exchange_writer import ExchangeWriter
from haversine import haversine


class DistanceCalculator:
    def __init__(self,
                 trips_input_queue_reader: QueueReader,
                 trips_output_exchange_writer: ExchangeWriter,
                 n_montreal_stations_joiners: int):
        self.trips_input_queue_reader = trips_input_queue_reader
        self.trips_output_exchange_writer = trips_output_exchange_writer
        self.n_montreal_stations_joiners = n_montreal_stations_joiners

        self.eofs_received = 0

    def run(self):
        self.trips_input_queue_reader.consume(callback=self.consume_messages)

    def consume_messages(self, _channel, _method, _properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.TRIPS_BATCH:
            self.calculate_distances(body)
        if message_type == common.network.constants.TRIPS_END_ALL:
            self.process_shutdown_signal()

    def calculate_distances(self, body):
        joined_trips = pickle.loads(body[common.network.constants.HEADER_TYPE_LEN:])

        distances_batch = []
        for trip, start_station, end_station in joined_trips:
            start = (start_station.latitude, start_station.longitude)
            end = (end_station.latitude, end_station.longitude)
            distance = haversine(start, end)
            distances_batch.append((end_station.name, distance))

        serialized_batch = common.network.constants.TRIPS_BATCH + pickle.dumps(distances_batch)
        self.trips_output_exchange_writer.write(serialized_batch)

    def process_shutdown_signal(self):
        self.eofs_received += 1
        if self.eofs_received == self.n_montreal_stations_joiners:
            self.trips_output_exchange_writer.write(common.network.constants.TRIPS_END_ALL)
            self.trips_input_queue_reader.shutdown()
