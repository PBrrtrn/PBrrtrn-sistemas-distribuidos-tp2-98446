from bully_node import BullyNode
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.queue import Queue
import common.env_utils


NETWORK_SIZE = 4  # TODO: Parametro


def main():
    config = common.env_utils.read_config()

    exchange_writer = ExchangeWriter(exchange_name=config['EXCHANGE_NAME'])

    queue_name = config['NODE_ID']
    queue = Queue(
        name=queue_name,
        bindings={config['EXCHANGE_NAME']: [queue_name]}
    )

    bully_node = BullyNode(
        exchange_writer=exchange_writer,
        queue=queue,
        node_id=int(config['NODE_ID']),
        network_size=NETWORK_SIZE
    )
    bully_node.start()


if __name__ == '__main__':
    main()
