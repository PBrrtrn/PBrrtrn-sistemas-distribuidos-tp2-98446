import signal

from supervisor_node import SupervisorNode
from common.rabbitmq.exchange_writer import ExchangeWriter
from supervisor_queue import SupervisorQueue
import common.env_utils


NETWORK_SIZE = 4  # TODO: Parametro


def main():
    config = common.env_utils.read_config()

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
        node_id=int(config['NODE_ID']),
        network_size=NETWORK_SIZE
    )

    signal.signal(signal.SIGINT, supervisor_node.exit_gracefully)
    signal.signal(signal.SIGTERM, supervisor_node.exit_gracefully)
    supervisor_node.start()


if __name__ == '__main__':
    main()
