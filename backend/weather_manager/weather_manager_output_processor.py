import pickle

from common.rabbitmq.queue import Queue
import common.network.deserialize


class WeatherManagerOutputProcessor:
    TRIP_DATE_LEN = 10

    def __init__(self, rpc_queue: Queue):
        self.rpc_queue = rpc_queue
        self.storage = {}

    def process_output(self, message: bytes, _method, _properties):
        message_type = message[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.WEATHER_BATCH:
            raw_batch = message[common.network.constants.HEADER_TYPE_LEN:]
            self.process_batch(raw_batch)

    def process_batch(self, raw_batch):
        weather_batch, city = pickle.loads(raw_batch)
        if city not in self.storage:
            self.storage[city] = {}

        for weather in weather_batch:
            self.storage[city][weather.date] = weather

    def finish_processing(self):
        for (method, properties, message) in self.rpc_queue.read_with_props():
            message_type = message[:common.network.constants.HEADER_TYPE_LEN]
            raw_message = message[common.network.constants.HEADER_TYPE_LEN:]
            if message_type == common.network.constants.TRIPS_BATCH:
                raw_batch, city = pickle.loads(raw_message)
                trips_batch = common.network.deserialize.deserialize_trips_batch(raw_batch)
                response = []
                for trip in trips_batch:
                    trip_date = trip.start_date[:self.TRIP_DATE_LEN]
                    if trip_date in self.storage[city]:
                        response.append(trip)

                serialized_response = pickle.dumps(response)
                self.rpc_queue.respond(
                    message=serialized_response,
                    to=properties.reply_to,
                    correlation_id=properties.correlation_id,
                    delivery_tag=method.delivery_tag
                )
            elif message_type == common.network.constants.TRIPS_END_ALL:
                self.rpc_queue.respond(
                    message=b'',
                    to=properties.reply_to,
                    correlation_id=properties.correlation_id,
                    delivery_tag=method.delivery_tag
                )

                self.rpc_queue.close()
