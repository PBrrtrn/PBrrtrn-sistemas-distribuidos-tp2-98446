import common.env_utils
from stations_manager_output_processor import StationsManagerOutputProcessor
from common.processing_node.identity_process_input import identity_process_input
from common.processing_node.processing_node import ProcessingNode
import common.network.constants
from common.rabbitmq.queue import Queue
from common.rabbitmq.queue_reader import QueueReader
from stations_manager import StationsManager


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['STATIONS_INPUT_QUEUE_BINDINGS'])
    stations_queue = Queue(
        hostname='rabbitmq',
        name=config['STATIONS_INPUT_QUEUE_NAME'],
        bindings=queue_bindings
    )

    rpc_queue_reader = QueueReader(queue_name=config['STATIONS_RPC_QUEUE_NAME'])
    stations_manager_output_processor = StationsManagerOutputProcessor(
        rpc_queue_reader=rpc_queue_reader,
        n_montreal_stations_joiners=int(config['N_MONTREAL_STATIONS_JOINERS'])
    )

    # Quedó deprecada la implementación del StationManager, siendo que este necesita un EOF por ciudad, por eso se
    # hardcodea que hay 3 input_peers cuando en realidad no los hay -- así, se espera a recibir tres EOF. Habría que
    # arreglarlo, pero está acoplado al protocolo (aunque no debería ser un cambio tan grande)
    processing_node = ProcessingNode(
        process_input=identity_process_input,
        input_eof=common.network.constants.STATIONS_END,
        n_input_peers=3,
        input_queue=stations_queue,
        output_processor=stations_manager_output_processor
    )

    processing_node.run()

# def main():
#     config = common.env_utils.read_config()
#
#     queue_bindings = common.env_utils.parse_queue_bindings(config['STATIONS_INPUT_QUEUE_BINDINGS'])
#     stations_queue_reader = QueueReader(
#         queue_name=config['STATIONS_INPUT_QUEUE_NAME'],
#         queue_bindings=queue_bindings)
#
#     rpc_queue_reader = QueueReader(queue_name=config['STATIONS_RPC_QUEUE_NAME'])
#
#     cities_to_manage = config['CITIES'].split(",")
#     n_montreal_stations_joiners = int(config['N_MONTREAL_STATIONS_JOINERS'])
#
#     stations_manager = StationsManager(
#         stations_queue_reader,
#         cities_to_manage,
#         rpc_queue_reader,
#         n_montreal_stations_joiners)
#
#     stations_manager.run()


if __name__ == "__main__":
    main()
