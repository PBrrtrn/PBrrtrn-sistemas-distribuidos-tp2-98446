import os
from configparser import ConfigParser

import common.env_utils
import common.network.constants
import common.supervisor.utils
from common.processing_node.queue_consumer.queue_consumer import QueueConsumer

from common.rabbitmq.queue import Queue
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.processing_node.queue_consumer.output_processor.forwarding_output_processor import ForwardingOutputProcessor
from common.processing_node.stateless_node import StatelessNode
from common.processing_node.queue_consumer.eof_handler import EOFHandler
from weather_filter_process_input import weather_filter_process_input
from common.processing_node.queue_consumer.client_list_storage_handler import ClientListStorageHandler

DIR = '.clients'
CLIENTS_LIST_FILENAME = 'clients_list'

def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def main():
    config = common.env_utils.read_config()

    queue_bindings = common.env_utils.parse_queue_bindings(config['UNFILTERED_WEATHER_QUEUE_BINDINGS'])
    input_queue_reader = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['UNFILTERED_WEATHER_QUEUE_NAME'],
        bindings=queue_bindings
    )

    output_exchange_writer = ExchangeWriter(
        hostname=config['RABBITMQ_HOSTNAME'],
        exchange_name=config['FILTERED_WEATHER_EXCHANGE_NAME'],
        queue_name=config['FILTERED_WEATHER_QUEUE_NAME']
    )

    clients_list_handler = ClientListStorageHandler(storage_directory=DIR, filename=CLIENTS_LIST_FILENAME)
    current_client_list = clients_list_handler.get_clients_list()
    forwarding_output_processor = ForwardingOutputProcessor(
        n_output_peers=1,
        output_exchange_writer=output_exchange_writer,
        output_eof=common.network.constants.WEATHER_END_ALL,
        forward_with_routing_key=True,
        current_client_list=current_client_list
    )

    eof_handlers_dict = {}
    for client_id in current_client_list:
        eof_handlers_dict[client_id] = EOFHandler('.eof', filename="eof_received", client_id=client_id)

    queue_consumer = QueueConsumer(
        process_input=weather_filter_process_input,
        input_eofs=[common.network.constants.WEATHER_END_ALL],
        n_input_peers=1,
        input_queue=input_queue_reader,
        output_processor=forwarding_output_processor,
        eof_handlers_dict=eof_handlers_dict
    )
    new_clients_queue_bindings = common.env_utils.parse_queue_bindings(config['NEW_CLIENTS_QUEUE_BINDINGS'])
    new_clients_queue = Queue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=config['NEW_CLIENTS_QUEUE_NAME'],
        bindings=new_clients_queue_bindings,
        exchange_type='fanout'
    )

    processing_node = StatelessNode(
        queue_consumer=queue_consumer,
        supervisor_process=common.supervisor.utils.create_from_config(config),
        clients_list_handler=clients_list_handler,
        new_clients_queue=new_clients_queue
    )

    processing_node.run()


if __name__ == "__main__":
    main()
