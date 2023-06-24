import pickle
from common.rabbitmq.queue import Queue


class StationDistanceStorageOutputProcessor:
    FILTER_DISTANCE = 6.0

    def __init__(self, rpc_queue: Queue):
        self.storage = {}
        self.rpc_queue = rpc_queue

    def process_output(self, message: bytes, _method, _properties):
        joined_trips_with_distance = pickle.loads(message)

        for end_station_name, distance in joined_trips_with_distance:
            if end_station_name in self.storage:
                self.storage[end_station_name]["total_distance"] += distance
                self.storage[end_station_name]["n_trips"] += 1
            else:
                self.storage[end_station_name] = {"total_distance": distance, "n_trips": 1}

    def finish_processing(self, _result, _method, _properties):
        for (method, properties, message) in self.rpc_queue.read_with_props():
            response = []
            for station_name, attributes in self.storage.items():
                avg_distance = attributes["total_distance"] / attributes["n_trips"]
                if avg_distance > self.FILTER_DISTANCE:
                    response.append(station_name)

            serialized_response = pickle.dumps(response)
            self.rpc_queue.respond(
                message=serialized_response,
                to=properties.reply_to,
                correlation_id=properties.correlation_id,
                delivery_tag=method.delivery_tag
            )

            self.rpc_queue.close()
