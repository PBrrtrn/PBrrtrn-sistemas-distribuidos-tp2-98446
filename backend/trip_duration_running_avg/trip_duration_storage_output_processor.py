import pickle
from common.rabbitmq.queue import Queue


class TripDurationStorageOutputProcessor:
    TRIP_DATE_LEN = 10

    def __init__(self, rpc_queue: Queue):
        self.total_duration = 0
        self.n_trips = 0
        self.rpc_queue = rpc_queue

    def process_output(self, message: bytes):
        trips = pickle.loads(message)
        for trip in trips:
            self.total_duration += trip.duration_sec
            self.n_trips += 1

    def finish_processing(self):
        for (method, properties, message) in self.rpc_queue.read_with_props():
            response = self.total_duration / self.n_trips

            serialized_response = pickle.dumps(response)
            self.rpc_queue.respond(
                message=serialized_response,
                to=properties.reply_to,
                correlation_id=properties.correlation_id,
                delivery_tag=method.delivery_tag
            )

            self.rpc_queue.close()