import signal

import docker

from supervisor_node import SupervisorNode
from common.rabbitmq.exchange_writer import ExchangeWriter
from supervisor_queue import SupervisorQueue
from common.supervisor.node_restarter import NodeRestarter
import common.env_utils


def main():
    config = common.env_utils.read_config()

    docker_client = docker.from_env()
    node_id_to_container_name_mapping = common.env_utils.parse_node_id_to_container_name_mapping(
        config['SUPERVISOR_ID_TO_CONTAINER_MAPPING']  # TODO: Mover a .env
    )
    node_restarter = NodeRestarter(docker_client, node_id_to_container_name_mapping)

    exchange_writer = ExchangeWriter(
        hostname=config['RABBITMQ_HOSTNAME'],
        exchange_name=config['EXCHANGE_NAME'])

    queue_name = config['NODE_ID']
    supervisor_queue = SupervisorQueue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=queue_name,
        bindings={config['EXCHANGE_NAME']: [queue_name]}
    )

    supervisor_node = SupervisorNode(
        exchange_writer=exchange_writer,
        queue=supervisor_queue,
        node_restarter=node_restarter,
        node_id=int(config['NODE_ID']),
        network_size=int(config['SUPERVISOR_NETWORK_SIZE']),  # TODO: Mover a .env
    )

    signal.signal(signal.SIGINT, supervisor_node.exit_gracefully)
    signal.signal(signal.SIGTERM, supervisor_node.exit_gracefully)
    supervisor_node.start()


if __name__ == '__main__':
    main()
