from common.rabbitmq.queue_reader import QueueReader
from stations_distance_running_avg import StationsDistanceRunningAvg
import common.env_utils


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['STATIONS_TRIP_DISTANCE_INPUT_QUEUE_BINDINGS'])
    stations_trip_distance_input_queue_reader = QueueReader(
        queue_name=config['STATIONS_TRIP_DISTANCE_INPUT_QUEUE_NAME'],
        queue_bindings=queue_bindings)

    rpc_queue_reader = QueueReader(queue_name=config['QUERY_RPC_QUEUE_NAME'])

    n_distance_calculators = int(config['N_DISTANCE_CALCULATORS'])

    stations_distance_running_avg = StationsDistanceRunningAvg(
        stations_trip_distance_input_queue_reader,
        rpc_queue_reader,
        n_distance_calculators
    )

    stations_distance_running_avg.run()


if __name__ == "__main__":
    main()
