import common.env_utils
import common.supervisor.utils
import common.network.constants
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.rabbitmq.queue import Queue
from common.processing_node.queue_consumer.process_input.identity_process_input import \
    identity_process_input
from common.processing_node.stateful_node import StatefulNode
from common.processing_node.queue_consumer.eof_handler import EOFHandler
from common.processing_node.queue_consumer.output_processor.storage_output_processor import StorageOutputProcessor
from rpc_duration_input_processor import RPCDurationInputProcessor
from trip_duration_storage_handler import TripDurationStorageHandler


def trip_duration_running_avg_queue_consumer_factory(client_id: str, config):
    trips_input_queue_bindings = common.env_utils.parse_queue_bindings_with_client_id(
        config["TRIPS_INPUT_QUEUE_BINDINGS"], client_id)
    trips_input_queue_name = config["TRIPS_INPUT_QUEUE_NAME"]
    trips_input_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=trips_input_queue_name + client_id,
        bindings=trips_input_queue_bindings
    )

    rpc_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config["RPC_QUEUE_NAME"] + client_id
    )
    rpc_input_processor = RPCDurationInputProcessor()
    storage_handler = TripDurationStorageHandler(
        storage_directory=config['STORAGE_PATH'],
        checkpoint_frequency=int(config['CHECKPOINT_FREQUENCY'])
    )
    storage_output_processor = StorageOutputProcessor(
        rpc_queue=rpc_queue_reader,
        storage_handler=storage_handler,
        finish_processing_node_args={
            'input_eofs': [common.network.constants.END_QUERY],
            'n_input_peers': 1,
            'rpc_input_processor': rpc_input_processor,
            'eof_handler': EOFHandler(".eof", append="_rpc")
        }
    )

    return QueueConsumer(
        process_input=identity_process_input,
        input_eofs=[common.network.constants.TRIPS_END_ALL],
        n_input_peers=1,
        input_queue=trips_input_queue_reader,
        output_processor=storage_output_processor,
        eof_handler=EOFHandler(".eof")
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
        queue_consumer_factory=trip_duration_running_avg_queue_consumer_factory,
        config=config
    )

    processing_node.run()


if __name__ == "__main__":
    main()
