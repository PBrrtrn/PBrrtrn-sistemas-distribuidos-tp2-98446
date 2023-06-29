import common.network.constants
import common.network.deserialize
from common.rabbitmq.rpc_client import RPCClient


class PrecipitationAvgDurationTripIngestorProcessor:
    def __init__(self, weather_rpc_client: RPCClient):
        self.weather_rpc_client = weather_rpc_client

    def process_input(self, message_type: bytes, message_body: bytes, client_id, message_id):
        if message_type == common.network.constants.TRIPS_BATCH:
            return self.weather_rpc_client.call(message_type + client_id.encode() + message_id.encode() + message_body,
                                                client_id)
