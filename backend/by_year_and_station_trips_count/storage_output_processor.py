import pickle
import common.network.constants
from common.rabbitmq.rpc_client import RPCClient
from common.rabbitmq.queue import Queue


class StorageOutputProcessor:
    def __init__(self, rpc_queue: Queue, rpc_client: RPCClient):
        self.storage = {}
        self.rpc_queue = rpc_queue
        self.rpc_client = rpc_client

    def process_output(self, message: bytes, _method, _properties):
        trips, city = pickle.loads(message)
        for trip in trips:
            if city not in self.storage:
                self.storage[city] = {}

            if trip.start_station_code not in self.storage[city]:
                self.storage[city][trip.start_station_code] = {'2016': 0, '2017': 0}

            trip_year = trip.start_date[:4]
            self.storage[city][trip.start_station_code][trip_year] += 1

    def finish_processing(self):
        for (method, properties, message) in self.rpc_queue.read_with_props():
            message_type = message[:common.network.constants.HEADER_TYPE_LEN]
            if message_type == common.network.constants.EXECUTE_QUERIES:
                response = []
                for city in self.storage.keys():
                    city_station_codes = []
                    for code, yearly_trips in self.storage[city].items():
                        if self.storage[city][code]['2017'] >= 2 * self.storage[city][code]['2016']:
                            city_station_codes.append(code)

                    join_request = common.network.constants.STATIONS_BATCH + pickle.dumps((city_station_codes, city))
                    response += pickle.loads(self.rpc_client.call(join_request))

                self.rpc_client.call(common.network.constants.STATIONS_END)

                serialized_response = pickle.dumps(response)
                self.rpc_queue.respond(
                    message=serialized_response,
                    to=properties.reply_to,
                    correlation_id=properties.correlation_id,
                    delivery_tag=method.delivery_tag
                )

                self.rpc_queue.close()