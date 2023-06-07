from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.exchange_writer import ExchangeWriter

import common.network.constants
import common.network.deserialize


class DoubledYearlyTripsStationsIngestor:
    def __init__(self, trips_input: QueueReader, trips_output: ExchangeWriter, n_by_year_filters: int):
        self.trips_input_queue_reader = trips_input
        self.trips_output_exchange_writer = trips_output
        self.n_by_year_filters = n_by_year_filters

    def run(self):
        self.trips_input_queue_reader.consume(self.consume_messages)

    def consume_messages(self, _channel, _method, _properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.TRIPS_BATCH:
            self.forward_trips_batch(body)
        elif message_type == common.network.constants.TRIPS_END_ALL:
            self.shutdown()
        elif message_type == common.network.constants.TRIPS_END:
            pass
        else:
            print(f"ERROR - Unexpected message type (got {message_type})")

    def forward_trips_batch(self, body):
        self.trips_output_exchange_writer.write(body)

    def shutdown(self):
        for _ in range(self.n_by_year_filters):
            self.trips_output_exchange_writer.write(common.network.constants.TRIPS_END_ALL)

        self.trips_input_queue_reader.shutdown()
