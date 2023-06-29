import common.network.constants
import common.network.deserialize
from common.rabbitmq.rpc_client import RPCClient


class PrecipitationAvgDurationTripIngestorProcessor:
    def __init__(self, weather_rpc_client: RPCClient):
        self.weather_rpc_client = weather_rpc_client

    def process_input(self, message_type: bytes, message_body: bytes, client_id):
        if message_type == common.network.constants.TRIPS_BATCH:
            response = self.weather_rpc_client.call(message_type + client_id.encode() + message_body, client_id)
            print(f"RESPUESTA WEATHER{response[:6]}")
            #if len(raw_filtered_trips) == 5:
            #    return None                     Esto haría que no se manden trips vacíos al TripDurationRunningAvg
            #else:
            return response
