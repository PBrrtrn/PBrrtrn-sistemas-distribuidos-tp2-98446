import signal
import docker
import common.env_utils
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.supervisor.supervisor_queue import SupervisorQueue
from common.supervisor.node_restarter import NodeRestarter
from common.supervisor.supervisor_process import SupervisorProcess


def create_from_config(config):
    docker_client = docker.from_env()
    node_id_to_container_name_mapping = common.env_utils.parse_node_id_to_container_name_mapping(
        config['SUPERVISOR_NODE_ID_TO_CONTAINER_NAME_MAPPING']
    )
    node_restarter = NodeRestarter(docker_client, node_id_to_container_name_mapping)

    exchange_writer = ExchangeWriter(
        hostname=config['RABBITMQ_HOSTNAME'],
        exchange_name=config['SUPERVISOR_EXCHANGE_NAME'])

    queue_name = config['SUPERVISOR_NODE_ID']
    supervisor_queue = SupervisorQueue(
        hostname=config['RABBITMQ_HOSTNAME'],
        name=queue_name,
        bindings={config['SUPERVISOR_EXCHANGE_NAME']: [queue_name]}
    )

    supervisor_process = SupervisorProcess(
        exchange_writer=exchange_writer,
        queue=supervisor_queue,
        node_restarter=node_restarter,
        node_id=int(config['SUPERVISOR_NODE_ID']),
        network_size=int(config['SUPERVISOR_NETWORK_SIZE'])
    )

    signal.signal(signal.SIGINT, supervisor_process.exit_gracefully)
    signal.signal(signal.SIGTERM, supervisor_process.exit_gracefully)

    return supervisor_process
