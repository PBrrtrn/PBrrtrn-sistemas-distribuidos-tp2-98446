import pickle

import common.network.constants
from common.rabbitmq.queue_reader import QueueReader


class StationsDistanceRunningAvg:
    FILTER_DISTANCE = 6.0

    def __init__(self,
                 stations_trip_distance_input_queue_reader: QueueReader,
                 rpc_queue_reader: QueueReader,
                 n_distance_calculators: int):
        self.stations_distance_input_queue_reader = stations_trip_distance_input_queue_reader
        self.rpc_queue_reader = rpc_queue_reader
        self.n_distance_calculators = n_distance_calculators

        self.storage = {}
        self.eofs_received = 0

    def run(self):
        self.stations_distance_input_queue_reader.consume(callback=self.consume_messages)
        self.rpc_queue_reader.consume(callback=self.respond_rpc, auto_ack=False)

    def consume_messages(self, _channel, _method, _properties, body):
        message_type = body[:common.network.constants.HEADER_TYPE_LEN]
        if message_type == common.network.constants.TRIPS_BATCH:
            self.process_trips_with_distances_batch(body)
        elif message_type == common.network.constants.TRIPS_END_ALL:
            self.process_shutdown_signal()
        else:
            print(f"ERROR - Unexpected message type ({message_type})")

    def process_trips_with_distances_batch(self, body):
        raw_message = body[common.network.constants.HEADER_TYPE_LEN:]
        joined_trips_with_distance = pickle.loads(raw_message)

        for end_station_name, distance in joined_trips_with_distance:
            if end_station_name in self.storage:
                self.storage[end_station_name]["total_distance"] += distance
                self.storage[end_station_name]["n_trips"] += 1
            else:
                self.storage[end_station_name] = {"total_distance": distance, "n_trips": 1}

    def process_shutdown_signal(self):
        self.eofs_received += 1
        if self.eofs_received == self.n_distance_calculators:
            self.stations_distance_input_queue_reader.shutdown()

    def respond_rpc(self, _channel, method, properties, _body):
        response = []
        for station_name, attributes in self.storage.items():
            avg_distance = attributes["total_distance"]/attributes["n_trips"]
            if avg_distance > self.FILTER_DISTANCE:
                response.append(station_name)

        serialized_response = pickle.dumps(response)
        self.rpc_queue_reader.respond(
            message=serialized_response,
            to=properties.reply_to,
            correlation_id=properties.correlation_id,
            delivery_tag=method.delivery_tag
        )

        self.rpc_queue_reader.shutdown()
