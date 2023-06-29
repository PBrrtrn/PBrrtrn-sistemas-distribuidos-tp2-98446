import pickle

import common.network.constants
import common.network.deserialize
from common.rabbitmq.rpc_client import RPCClient


class MontrealJoinerInputProcessor:
    MONTREAL = 'montreal'

    def __init__(self, stations_join_rpc_client: RPCClient):
        self.stations_join_rpc_client = stations_join_rpc_client

    def process_input(self, message_type: bytes, message_body: bytes):
        if message_type == common.network.constants.TRIPS_BATCH:
            message = pickle.loads(message_body)
            city = message[1]
            if city == self.MONTREAL:
                join_trips_request = common.network.constants.TRIPS_BATCH + message_body
                raw_joined_montreal_trips = self.stations_join_rpc_client.call(
                    join_trips_request, routing_key_suffix='1'
                )
                return common.network.constants.TRIPS_BATCH + raw_joined_montreal_trips
            