import pickle

from common.rabbitmq.queue_reader import QueueReader

import common.network.constants
import common.network.deserialize


class WeatherManager:
    TRIP_DATE_LEN = 10

    def __init__(self, weather_input_queue: QueueReader, n_weather_filters: int, rpc_queue_reader: QueueReader):
        self.weather_input_queue = weather_input_queue
        self.n_weather_filters = n_weather_filters
        self.rpc_queue_reader = rpc_queue_reader

        self.storage = {}
        self.eofs_received = 0

    def run(self):
        self.weather_input_queue.consume(callback=self.receive_weather_batches)
        self.rpc_queue_reader.consume(callback=self.respond_rpc, auto_ack=False)

    def receive_weather_batches(self, _channel, method, properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.WEATHER_BATCH:
            raw_batch = body[common.network.constants.HEADER_TYPE_LEN:]
            self.process_batch(raw_batch)
        elif message_type == common.network.constants.WEATHER_END_ALL:
            self.eofs_received += 1
            if self.eofs_received == self.n_weather_filters:
                self.weather_input_queue.shutdown()

    def process_batch(self, raw_batch):
        weather_batch, city = pickle.loads(raw_batch)
        if city not in self.storage:
            self.storage[city] = {}

        for weather in weather_batch:
            self.storage[city][weather.date] = weather

    def respond_rpc(self, _channel, method, properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        raw_message = body[common.network.constants.HEADER_TYPE_LEN:]
        if message_type == common.network.constants.TRIPS_BATCH:
            raw_batch, city = pickle.loads(raw_message)
            trips_batch = common.network.deserialize.deserialize_trips_batch(raw_batch)
            response = []
            for trip in trips_batch:
                trip_date = trip.start_date[:self.TRIP_DATE_LEN]
                if trip_date in self.storage[city]:
                    response.append(trip)

            serialized_response = pickle.dumps(response)
            self.rpc_queue_reader.respond(
                message=serialized_response,
                to=properties.reply_to,
                correlation_id=properties.correlation_id,
                delivery_tag=method.delivery_tag
            )
        elif message_type == common.network.constants.TRIPS_END_ALL:
            self.rpc_queue_reader.respond(
                message=b'',
                to=properties.reply_to,
                correlation_id=properties.correlation_id,
                delivery_tag=method.delivery_tag
            )

            self.rpc_queue_reader.shutdown()
