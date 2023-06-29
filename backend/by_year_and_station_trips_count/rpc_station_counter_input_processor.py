import pickle

from common.rabbitmq.rpc_client import RPCClient
import common.network.constants

class RPCStationCounterInputProcessor:
    def __init__(self, rpc_client: RPCClient):
        self.rpc_client = rpc_client
        self.storage = None

    def set_storage(self, storage):
        self.storage = storage

    def process_input(self, message_type: bytes, _message_body: bytes, client_id):
        if message_type == common.network.constants.EXECUTE_QUERIES:
            doubled_stations = []
            for city in self.storage.keys():
                city_station_codes = []
                for code, yearly_trips in self.storage[city].items():
                    if self.storage[city][code]['2017'] >= 2 * self.storage[city][code]['2016']:
                        city_station_codes.append(code)

                print("HERE! CALLING RPC CLIENT")
                join_request = common.network.constants.STATIONS_BATCH + client_id.encode() \
                               + pickle.dumps((city_station_codes, city))
                rpc_response = self.rpc_client.call(join_request)
                doubled_stations += pickle.loads(rpc_response[4:])
                print("OBTAINED RPC CLIENT")
            return common.network.constants.EXECUTE_QUERIES + client_id.encode() + pickle.dumps(doubled_stations)

