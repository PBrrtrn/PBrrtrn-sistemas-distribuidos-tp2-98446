import pickle

from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.rpc_client import RPCClient
from common.rabbitmq.exchange_writer import ExchangeWriter

import common.network.constants


class MontrealStationsJoiner:
    MONTREAL = 'montreal'

    def __init__(self,
                 trips_input: QueueReader,
                 stations_join: RPCClient,
                 trips_output: ExchangeWriter,
                 n_distance_calculators: int):
        self.trips_input_queue_reader = trips_input
        self.stations_join_rpc_client = stations_join
        self.trips_output_exchange_writer = trips_output
        self.n_distance_calculators = n_distance_calculators

    def run(self):
        self.trips_input_queue_reader.consume(callback=self.consume_messages)

    def consume_messages(self, _channel, _method, _properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.TRIPS_BATCH:
            self.process_trips_batch(body[common.network.constants.HEADER_TYPE_LEN:])
        elif message_type == common.network.constants.TRIPS_END_ALL:
            self.shutdown()

    def process_trips_batch(self, raw_trips_batch):
        message = pickle.loads(raw_trips_batch)
        city = message[1]
        if city == self.MONTREAL:
            join_trips_request = common.network.constants.TRIPS_BATCH + raw_trips_batch
            raw_joined_montreal_trips = self.stations_join_rpc_client.call(join_trips_request)
            trips_output = common.network.constants.TRIPS_BATCH + raw_joined_montreal_trips
            self.trips_output_exchange_writer.write(trips_output)

    def shutdown(self):
        for _ in range(self.n_distance_calculators):
            self.trips_output_exchange_writer.write(common.network.constants.TRIPS_END_ALL)

        self.stations_join_rpc_client.call(common.network.constants.TRIPS_END_ALL)
        self.trips_input_queue_reader.shutdown()
