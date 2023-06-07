from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.rpc_client import RPCClient
import common.network.constants


class WithPrecipitationsAvgDurationTripIngestor:
    def __init__(self,
                 trips_input_queue_reader: QueueReader,
                 weather_rpc_client: RPCClient,
                 duration_running_avg_exchange_writer: ExchangeWriter):
        self.trips_input_queue_reader = trips_input_queue_reader
        self.weather_rpc_client = weather_rpc_client
        self.duration_running_avg_exchange_writer = duration_running_avg_exchange_writer

    def run(self):
        self.trips_input_queue_reader.consume(self.process_trips_batch)

    def process_trips_batch(self, _channel, _method, _properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.TRIPS_BATCH:
            raw_filtered_trips = self.weather_rpc_client.call(body)
            message = common.network.constants.TRIPS_BATCH + raw_filtered_trips
            self.duration_running_avg_exchange_writer.write(message)
        elif message_type == common.network.constants.TRIPS_END_ALL:
            self.weather_rpc_client.call(message_type)
            self.duration_running_avg_exchange_writer.write(message_type)
            self.trips_input_queue_reader.shutdown()
