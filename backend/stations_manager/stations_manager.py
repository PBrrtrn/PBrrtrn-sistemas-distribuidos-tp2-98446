import pickle
from typing import List

import common.network.constants
import common.network.deserialize
from common.rabbitmq.queue_reader import QueueReader


class StationsManager:
    def __init__(self, stations_input_queue_reader: QueueReader,
                 cities_to_manage: List[str],
                 rpc_queue_reader: QueueReader,
                 n_montreal_stations_joiners: int):
        self.storage = {}
        for city in cities_to_manage:
            self.storage[city] = {}

        self.pending_eofs = set(cities_to_manage)
        self.stations_input_queue_reader = stations_input_queue_reader
        self.rpc_queue_reader = rpc_queue_reader

        self.n_montreal_stations_joiners = n_montreal_stations_joiners

        self.shutdown_signals = 0

    def run(self):
        self.stations_input_queue_reader.consume(callback=self.receive_stations_batch)
        self.rpc_queue_reader.consume(callback=self.respond_rpc, auto_ack=False)

    def receive_stations_batch(self, _channel, _method, _properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        raw_message = body[common.network.constants.HEADER_TYPE_LEN:]
        if message_type == common.network.constants.STATIONS_BATCH:
            self.process_batch(raw_message)
        elif message_type == common.network.constants.STATIONS_END:
            message_city = raw_message.decode('utf-8')
            self.pending_eofs.remove(message_city)
            if len(self.pending_eofs) == 0:
                self.stations_input_queue_reader.shutdown()

    def process_batch(self, raw_batch):
        message = pickle.loads(raw_batch)
        raw_batch = message[0]
        city = message[1]
        batch = common.network.deserialize.deserialize_stations_batch(raw_batch)
        for station in batch:
            self.storage[city][station.code] = station

    def respond_rpc(self, _channel, method, properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        raw_message = body[common.network.constants.HEADER_TYPE_LEN:]
        if message_type == common.network.constants.TRIPS_BATCH:
            self.join_trips(method, properties, raw_message)
        elif message_type == common.network.constants.STATIONS_BATCH:
            self.join_stations(method, properties, raw_message)
        elif message_type in [common.network.constants.TRIPS_END_ALL, common.network.constants.STATIONS_END]:
            self.process_shutdown_signal(method, properties)
        else:
            print(f"ERROR - Unknown message header (got {message_type})")

    def process_shutdown_signal(self, method, properties):
        self.rpc_queue_reader.respond(
            message=b'',
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
            delivery_tag=method.delivery_tag
        )
        self.shutdown_signals += 1
        if self.shutdown_signals >= 1 + self.n_montreal_stations_joiners:
            self.rpc_queue_reader.shutdown()

    def join_stations(self, method, properties, raw_message):
        station_codes, city = pickle.loads(raw_message)
        response = []
        for station_code in station_codes:
            station = self.storage[city][station_code]
            response.append(station.name)
        serialized_response = pickle.dumps(response)
        self.rpc_queue_reader.respond(
            message=serialized_response,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
            delivery_tag=method.delivery_tag
        )

    def join_trips(self, method, properties, raw_message):
        raw_batch, city = pickle.loads(raw_message)
        trips_batch = common.network.deserialize.deserialize_trips_batch(raw_batch)
        response = []
        for trip in trips_batch:
            if trip.start_station_code in self.storage[city] and trip.end_station_code in self.storage[city]:
                start_station = self.storage[city][trip.start_station_code]
                end_station = self.storage[city][trip.end_station_code]
                response.append((trip, start_station, end_station))
        serialized_response = pickle.dumps(response)
        self.rpc_queue_reader.respond(
            message=serialized_response,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
            delivery_tag=method.delivery_tag
        )
