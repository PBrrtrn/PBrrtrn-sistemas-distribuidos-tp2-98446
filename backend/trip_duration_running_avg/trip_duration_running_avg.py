import pickle

import common.network.constants

from common.rabbitmq.queue_reader import QueueReader


class TripDurationRunningAvg:
    TRIP_DATE_LEN = 10

    def __init__(self, trips_input_queue_reader: QueueReader, rpc_queue_reader: QueueReader):
        self.trips_input_queue_reader = trips_input_queue_reader
        self.rpc_queue_reader = rpc_queue_reader

        self.total_duration = 0
        self.n_trips = 0

    def run(self):
        self.trips_input_queue_reader.consume(callback=self.receive_trips_batches)
        self.rpc_queue_reader.consume(callback=self.respond_rpc, auto_ack=False)

    def receive_trips_batches(self, _channel, _method, _properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.TRIPS_BATCH:
            self.process_trips_batch(body[common.network.constants.HEADER_TYPE_LEN:])
        if message_type == common.network.constants.TRIPS_END_ALL:
            self.trips_input_queue_reader.shutdown()

    def process_trips_batch(self, raw_batch):
        trips = pickle.loads(raw_batch)
        for trip in trips:
            self.total_duration += trip.duration_sec
            self.n_trips += 1

    def respond_rpc(self, _channel, method, properties, _body):
        response = self.total_duration / self.n_trips

        serialized_response = pickle.dumps(response)
        self.rpc_queue_reader.respond(
            message=serialized_response,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
            delivery_tag=method.delivery_tag
        )

        self.rpc_queue_reader.shutdown()
