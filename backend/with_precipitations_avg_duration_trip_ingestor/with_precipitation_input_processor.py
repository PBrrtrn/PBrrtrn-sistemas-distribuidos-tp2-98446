import common.network.constants
import common.network.deserialize
from common.rabbitmq.rpc_client import RPCClient


class PrecipitationAvgDurationTripIngestorProcessor:
    def __init__(self, weather_rpc_client: RPCClient):
        self.weather_rpc_client = weather_rpc_client

    def process_input(self, message_type: bytes, message_body: bytes):
        if message_type == common.network.constants.TRIPS_BATCH:
            raw_filtered_trips = self.weather_rpc_client.call(message_type + message_body, "1")
            #if len(raw_filtered_trips) == 5:
            #    return None                     Esto haría que no se manden trips vacíos al TripDurationRunningAvg
            #else:
            return common.network.constants.TRIPS_BATCH + raw_filtered_trips
