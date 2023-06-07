from trip_duration_running_avg import TripDurationRunningAvg
import common.env_utils
from common.rabbitmq.queue_reader import QueueReader


def main():
    config = common.env_utils.read_config()

    trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config["TRIPS_INPUT_QUEUE_BINDINGS"])
    trips_input_queue_name = config["TRIPS_INPUT_QUEUE_NAME"]
    trips_input_queue_reader = QueueReader(
        queue_name=trips_input_queue_name,
        queue_bindings=trips_input_queue_bindings
    )

    rpc_queue_reader = QueueReader(queue_name=config["RPC_QUEUE_NAME"])

    by_date_duration_running_avg = TripDurationRunningAvg(trips_input_queue_reader, rpc_queue_reader)
    by_date_duration_running_avg.run()


if __name__ == "__main__":
    main()
