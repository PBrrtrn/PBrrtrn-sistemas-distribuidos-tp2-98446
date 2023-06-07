import pickle

from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.rpc_client import RPCClient

import common.network.constants


class ByYearAndStationTripsCount:
    def __init__(self,
                 input_queue_reader: QueueReader,
                 rpc_queue_reader: QueueReader,
                 rpc_client: RPCClient,
                 n_by_year_trips_filters: int):
        self.input_queue_reader = input_queue_reader
        self.rpc_queue_reader = rpc_queue_reader
        self.rpc_client = rpc_client
        self.n_by_year_trips_filters = n_by_year_trips_filters

        self.received_eofs = 0
        self.storage = {}

    def run(self):
        self.input_queue_reader.consume(callback=self.receive_trip_batches)
        self.rpc_queue_reader.consume(self.respond_rpc, auto_ack=False)

    def receive_trip_batches(self,  channel, method, properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.TRIPS_BATCH:
            raw_contents = body[common.network.constants.HEADER_TYPE_LEN:]
            self.process_trips_batch(raw_contents)
        elif message_type == common.network.constants.TRIPS_END_ALL:
            self.shutdown()

    def process_trips_batch(self, raw_contents):
        trips, city = pickle.loads(raw_contents)
        for trip in trips:
            if city not in self.storage:
                self.storage[city] = {}

            if trip.start_station_code not in self.storage[city]:
                self.storage[city][trip.start_station_code] = {'2016': 0, '2017': 0}

            trip_year = trip.start_date[:4]
            self.storage[city][trip.start_station_code][trip_year] += 1

    def shutdown(self):
        self.received_eofs += 1
        if self.received_eofs == self.n_by_year_trips_filters:
            self.input_queue_reader.shutdown()

    def respond_rpc(self, _channel, method, properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.EXECUTE_QUERIES:
            response = []
            for city in self.storage.keys():
                city_station_codes = []
                for code, yearly_trips in self.storage[city].items():
                    if self.storage[city][code]['2017'] >= 2*self.storage[city][code]['2016']:
                        city_station_codes.append(code)

                join_request = common.network.constants.STATIONS_BATCH + pickle.dumps((city_station_codes, city))
                response += pickle.loads(self.rpc_client.call(join_request))

            self.rpc_client.call(common.network.constants.STATIONS_END)

            serialized_response = pickle.dumps(response)
            self.rpc_queue_reader.respond(
                message=serialized_response,
                to=properties.reply_to,
                correlation_id=properties.correlation_id,
                delivery_tag=method.delivery_tag
            )

            self.rpc_queue_reader.shutdown()
