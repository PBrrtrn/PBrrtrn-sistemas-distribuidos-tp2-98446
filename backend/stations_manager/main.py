import common.env_utils
import common.supervisor.utils

from common.processing_node.queue_consumer.process_input.identity_process_input import identity_process_input
from common.processing_node.stateful_node import StatefulNode
from common.processing_node.queue_consumer.output_processor.storage_output_processor import StorageOutputProcessor
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.processing_node.queue_consumer.eof_handler import EOFHandler
from rpc_station_input_processor import RPCStationInputProcessor
from station_storage_handler import StationStorageHandler
import common.network.constants
from common.rabbitmq.queue import Queue


def stations_manager_queue_consumer_factory(client_id: str, config):
    queue_bindings = common.env_utils.parse_queue_bindings_with_client_id(
        config['STATIONS_INPUT_QUEUE_BINDINGS'], client_id)
    stations_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['STATIONS_INPUT_QUEUE_NAME'] + client_id,
        bindings=queue_bindings
    )

    requests_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['STATIONS_RPC_QUEUE_NAME'] + client_id
    )

    rpc_input_processor = RPCStationInputProcessor()
    storage_handler = StationStorageHandler(
        storage_directory=config['STORAGE_PATH'],
        checkpoint_frequency=int(config['CHECKPOINT_FREQUENCY']),
        client_id=client_id
    )
    storage_output_processor = StorageOutputProcessor(
        rpc_queue=requests_queue_reader,
        storage_handler=storage_handler,
        finish_processing_node_args={
            'input_eofs': [common.network.constants.STATIONS_END, common.network.constants.TRIPS_END_ALL],
            'n_input_peers': int(config['N_MONTREAL_STATIONS_JOINERS']) + 1,
            'rpc_input_processor': rpc_input_processor,
            'eof_handler': EOFHandler(".eof", append=f"_rpc_{client_id}")
        }
    )

    # Quedó deprecada la implementación del StationManager, siendo que este necesita un EOF por ciudad, por eso se
    # hardcodea que hay 3 input_peers cuando en realidad no los hay -- así, se espera a recibir tres EOF. Habría que
    # arreglarlo, pero está acoplado al protocolo (aunque no debería ser un cambio tan grande)
    return QueueConsumer(
        process_input=identity_process_input,
        input_eofs=[common.network.constants.STATIONS_END],
        n_input_peers=3,
        input_queue=stations_queue,
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
        queue_consumer_factory=stations_manager_queue_consumer_factory,
        config=config
    )

    processing_node.run()


if __name__ == "__main__":
    main()
