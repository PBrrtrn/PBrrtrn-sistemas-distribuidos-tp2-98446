import pickle

from common.rabbitmq.queue_reader import QueueReader
from common.rabbitmq.exchange_writer import ExchangeWriter

import common.network.constants
import common.network.deserialize
import common.network.serialize


class WeatherFilter:
    FILTERED_BATCH_SIZE = 10

    def __init__(self, input_queue: QueueReader, output_queue: ExchangeWriter):
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self):
        self.input_queue.consume(self.process_message)

    def process_message(self, _channel, method, _properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.WEATHER_BATCH:
            raw_message = body[common.network.constants.HEADER_TYPE_LEN:]
            raw_batch, city = pickle.loads(raw_message)
            weather_batch = common.network.deserialize.deserialize_weather_batch(raw_batch)

            filtered_weathers = []
            for weather in weather_batch:
                if weather.precipitations >= 30.0:
                    filtered_weathers.append(weather)

            serialized_message = pickle.dumps((filtered_weathers, city))
            self.output_queue.write(common.network.constants.WEATHER_BATCH + serialized_message)
        elif message_type == common.network.constants.WEATHER_END_ALL:
            self.output_queue.write(message_type)

            self.input_queue.shutdown()
        else:
            print(f"ERROR - Unexpected message type ({message_type})")
