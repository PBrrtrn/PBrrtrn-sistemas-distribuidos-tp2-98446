import common.env_utils
import common.network.constants
import common.supervisor.utils
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer

from common.rabbitmq.queue import Queue
from common.processing_node.queue_consumer.process_input.identity_process_input import identity_process_input
from common.processing_node.stateful_node import StatefulNode
from common.processing_node.queue_consumer.output_processor.storage_output_processor import StorageOutputProcessor
from common.processing_node.queue_consumer.eof_handler import EOFHandler
from station_distance_storage_handler import StationDistanceStorageHandler
from rpc_distance_input_processor import RPCDistanceInputProcessor


def stations_distance_running_avg_queue_consumer_factory(client_id: str, config):
    queue_bindings = common.env_utils.parse_queue_bindings_with_client_id(
        config['STATIONS_TRIP_DISTANCE_INPUT_QUEUE_BINDINGS'], client_id)
    stations_trip_distance_input_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['STATIONS_TRIP_DISTANCE_INPUT_QUEUE_NAME'] + client_id,
        bindings=queue_bindings
    )

    rpc_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['QUERY_RPC_QUEUE_NAME'] + client_id
    )
    rpc_input_processor = RPCDistanceInputProcessor()
    storage_handler = StationDistanceStorageHandler(
        storage_directory=config['STORAGE_PATH'],
        checkpoint_frequency=int(config['CHECKPOINT_FREQUENCY']),
        client_id=client_id
    )
    storage_output_processor = StorageOutputProcessor(
        rpc_queue=rpc_queue_reader,
        storage_handler=storage_handler,
        finish_processing_node_args={
            'input_eofs': [common.network.constants.END_QUERY],
            'n_input_peers': 1,
            'rpc_input_processor': rpc_input_processor,
            'eof_handler': EOFHandler(".eof", append=f"_rpc_{client_id}")
        }
    )

    return QueueConsumer(
        process_input=identity_process_input,
        input_eofs=[common.network.constants.TRIPS_END_ALL],
        n_input_peers=int(config['N_DISTANCE_CALCULATORS']),
        input_queue=stations_trip_distance_input_queue_reader,
        output_processor=storage_output_processor,
        eof_handler=EOFHandler(".eof", append=f"_{client_id}")
    )


def main():
    config = common.env_utils.read_config()

    new_clients_queue_bindings = common.env_utils.parse_queue_bindings(config['NEW_CLIENTS_QUEUE_BINDINGS'])

    new_clients_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['NEW_CLIENTS_QUEUE_NAME'],
        bindings=new_clients_queue_bindings,
        exchange_type='fanout'
    )

    processing_node = StatefulNode(
        supervisor_process=common.supervisor.utils.create_from_config(config),
        new_clients_queue=new_clients_queue,
        queue_consumer_factory=stations_distance_running_avg_queue_consumer_factory,
        config=config
    )

    processing_node.run()


if __name__ == "__main__":
    main()
