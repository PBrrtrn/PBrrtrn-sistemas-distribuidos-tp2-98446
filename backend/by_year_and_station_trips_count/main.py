import common.env_utils
import common.supervisor.utils
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.rabbitmq.queue import Queue
from common.processing_node.stateful_node import StatefulNode
from common.processing_node.queue_consumer.process_input.identity_process_input import \
    identity_process_input
from common.rabbitmq.rpc_client import RPCClient
from common.processing_node.queue_consumer.output_processor.storage_output_processor import StorageOutputProcessor
from station_counter_storage_handler import StationCounterStorageHandler
from rpc_station_counter_input_processor import RPCStationCounterInputProcessor
from common.processing_node.queue_consumer.eof_handler import EOFHandler
import common.network.constants


def by_year_and_stations_trip_count_queue_consumer_factory(client_id: str, config):
    filtered_trips_input_queue_bindings = common.env_utils.parse_queue_bindings_with_client_id(
        config['FILTERED_TRIPS_QUEUE_BINDINGS'], client_id)
    filtered_trips_input_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['FILTERED_TRIPS_QUEUE_NAME'] + client_id,
        bindings=filtered_trips_input_queue_bindings
    )

    requests_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['DOUBLED_YEARLY_TRIPS_STATIONS_RPC_QUEUE_NAME'] + client_id
    )
    stations_rpc_client = RPCClient(rpc_queue_name=config['STATIONS_RPC_QUEUE_NAME'] + client_id)
    rpc_input_processor = RPCStationCounterInputProcessor(rpc_client=stations_rpc_client)
    storage_handler = StationCounterStorageHandler(
        storage_directory=config['STORAGE_PATH'],
        checkpoint_frequency=int(config['CHECKPOINT_FREQUENCY']),
        client_id=client_id
    )
    storage_output_processor = StorageOutputProcessor(
        rpc_queue=requests_queue_reader,
        storage_handler=storage_handler,
        finish_processing_node_args={
            'input_eofs': [common.network.constants.END_QUERY],
            'n_input_peers': 1,
            'rpc_input_processor': rpc_input_processor,
            'eof_handler': EOFHandler(storage_directory=".eof", filename=f"eof_received_rpc_{client_id}"),
            'optional_rpc_eof': stations_rpc_client,
            'optional_rpc_eof_byte': common.network.constants.STATIONS_END
        }
    )

    return QueueConsumer(
        process_input=identity_process_input,
        input_eofs=[common.network.constants.TRIPS_END_ALL],
        n_input_peers=int(config['N_BY_YEAR_TRIPS_FILTERS']),
        input_queue=filtered_trips_input_queue_reader,
        output_processor=storage_output_processor,
        eof_handler=EOFHandler(".eof", filename=f"eof_received_{client_id}")
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
        queue_consumer_factory=by_year_and_stations_trip_count_queue_consumer_factory,
        config=config
    )

    processing_node.run()


if __name__ == "__main__":
    main()
