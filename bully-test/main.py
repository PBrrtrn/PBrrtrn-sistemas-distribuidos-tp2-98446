from bully_node import BullyNode
from common.rabbitmq.exchange_writer import ExchangeWriter
from common.rabbitmq.queue_reader import QueueReader
import common.env_utils


N_PEERS = 4


def main():
    config = common.env_utils.read_config()

    exchange_writer = ExchangeWriter(exchange_name=config['EXCHANGE_NAME'])

    queue_name = config['NODE_ID']
    queue_reader = QueueReader(
        queue_name=queue_name,
        queue_bindings={config['EXCHANGE_NAME']: [queue_name]}
    )

    bully_node = BullyNode(
        exchange_writer=exchange_writer,
        queue_reader=queue_reader,
        node_id=int(config['NODE_ID']),
        n_peers=N_PEERS
    )
    bully_node.start()


if __name__ == '__main__':
    main()
