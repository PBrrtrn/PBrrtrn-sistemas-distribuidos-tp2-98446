import common.network.constants
import common.network.deserialize
from common.rabbitmq.rpc_client import RPCClient


class PrecipitationAvgDurationTripIngestorProcessor:
    def __init__(self, weather_rpc_client: RPCClient):
        self.weather_rpc_client = weather_rpc_client

    def process_input(self, message_type: bytes, message_body: bytes):
        if message_type == common.network.constants.TRIPS_BATCH:
            raw_filtered_trips = self.weather_rpc_client.call(message_type + message_body)
            return common.network.constants.TRIPS_BATCH + raw_filtered_trips
        elif message_type == common.network.constants.TRIPS_END_ALL:
            self.weather_rpc_client.call(message_type)
