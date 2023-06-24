import pickle

from common.rabbitmq.rpc_client import RPCClient
import common.network.constants

class RPCStationInputProcessor:
    def __init__(self, rpc_client: RPCClient):
        self.rpc_client = rpc_client
        self.storage = None

    def set_storage(self, storage):
        self.storage = storage

    def process_input(self, message_type: bytes, _message_body: bytes):
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
            return pickle.dumps(response)

