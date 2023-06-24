import common.env_utils
from common.processing_node.identity_process_input import identity_process_input_without_header
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.storage_output_processor import StorageOutputProcessor
from rpc_station_input_processor import RPCStationInputProcessor
from station_storage_handler import StationStorageHandler
import common.network.constants
from common.rabbitmq.queue import Queue


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['STATIONS_INPUT_QUEUE_BINDINGS'])
    stations_queue = Queue(
        hostname='rabbitmq',
        name=config['STATIONS_INPUT_QUEUE_NAME'],
        bindings=queue_bindings
    )

    requests_queue_reader = Queue(
        hostname='rabbitmq',
        name=config['STATIONS_RPC_QUEUE_NAME']
    )

    rpc_input_processor = RPCStationInputProcessor()
    storage_handler = StationStorageHandler()
    storage_output_processor = StorageOutputProcessor(
        rpc_queue=requests_queue_reader,
        storage_handler=storage_handler,
        finish_processing_node_args={
            'input_eof': common.network.constants.EXECUTE_QUERIES,
            'n_input_peers': int(config['N_MONTREAL_STATIONS_JOINERS']),
            'rpc_input_processor': rpc_input_processor
        }
    )
    """stations_manager_output_processor = StationsManagerOutputProcessor(
        rpc_queue=rpc_queue,
        n_montreal_stations_joiners=int(config['N_MONTREAL_STATIONS_JOINERS'])
    )"""

    # Quedó deprecada la implementación del StationManager, siendo que este necesita un EOF por ciudad, por eso se
    # hardcodea que hay 3 input_peers cuando en realidad no los hay -- así, se espera a recibir tres EOF. Habría que
    # arreglarlo, pero está acoplado al protocolo (aunque no debería ser un cambio tan grande)
    processing_node = ProcessingNode(
        process_input=identity_process_input_without_header,
        input_eof=common.network.constants.STATIONS_END,
        n_input_peers=3,
        input_queue=stations_queue,
        output_processor=storage_output_processor
    )

    processing_node.run()


if __name__ == "__main__":
    main()
