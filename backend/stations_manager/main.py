import common.env_utils
from common.rabbitmq.queue_reader import QueueReader
from stations_manager import StationsManager


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['STATIONS_INPUT_QUEUE_BINDINGS'])
    stations_queue_reader = QueueReader(
        queue_name=config['STATIONS_INPUT_QUEUE_NAME'],
        queue_bindings=queue_bindings)

    rpc_queue_reader = QueueReader(queue_name=config['STATIONS_RPC_QUEUE_NAME'])

    cities_to_manage = config['CITIES'].split(",")
    n_montreal_stations_joiners = int(config['N_MONTREAL_STATIONS_JOINERS'])

    stations_manager = StationsManager(
        stations_queue_reader,
        cities_to_manage,
        rpc_queue_reader,
        n_montreal_stations_joiners)

    stations_manager.run()


if __name__ == "__main__":
    main()
