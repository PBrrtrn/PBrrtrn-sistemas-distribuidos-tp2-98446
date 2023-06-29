import common.env_utils
import common.supervisor.utils
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer
from common.rabbitmq.queue import Queue
from common.processing_node.processing_node import ProcessingNode
from common.processing_node.queue_consumer.process_input.identity_process_input import identity_process_input_without_header
from common.rabbitmq.rpc_client import RPCClient
from common.processing_node.queue_consumer.output_processor.storage_output_processor import StorageOutputProcessor
from station_counter_storage_handler import StationCounterStorageHandler
from rpc_station_counter_input_processor import RPCStationCounterInputProcessor
from common.processing_node.queue_consumer.eof_handler import EOFHandler
import common.network.constants


def main():
    config = common.env_utils.read_config()

    filtered_trips_input_queue_bindings = common.env_utils.parse_queue_bindings(config['FILTERED_TRIPS_QUEUE_BINDINGS'])
    filtered_trips_input_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['FILTERED_TRIPS_QUEUE_NAME'],
        bindings=filtered_trips_input_queue_bindings
    )

    requests_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['DOUBLED_YEARLY_TRIPS_STATIONS_RPC_QUEUE_NAME']
    )
    stations_rpc_client = RPCClient(rpc_queue_name=config['STATIONS_RPC_QUEUE_NAME'])
    rpc_input_processor = RPCStationCounterInputProcessor(rpc_client=stations_rpc_client)
    storage_handler = StationCounterStorageHandler(storage_directory=config['STORAGE_PATH'])
    storage_output_processor = StorageOutputProcessor(
        rpc_queue=requests_queue_reader,
        storage_handler=storage_handler,
        finish_processing_node_args={
            'input_eofs': [common.network.constants.END_QUERY],
            'n_input_peers': 1,
            'rpc_input_processor': rpc_input_processor,
            'eof_handler': EOFHandler(".eof", append="_rpc")
        }
    )

    queue_consumer = QueueConsumer(
        process_input=identity_process_input_without_header,
        input_eofs=[common.network.constants.TRIPS_END_ALL],
        n_input_peers=int(config['N_BY_YEAR_TRIPS_FILTERS']),
        input_queue=filtered_trips_input_queue_reader,
        output_processor=storage_output_processor,
        eof_handler=EOFHandler(".eof")
    )

    processing_node = ProcessingNode(
        queue_consumer=queue_consumer,
        supervisor_process=common.supervisor.utils.create_from_config(config)
    )

    processing_node.run()


if __name__ == "__main__":
    main()
