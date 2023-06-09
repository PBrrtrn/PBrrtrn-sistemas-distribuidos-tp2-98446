import struct

ELECTION_MESSAGE = '0'


def election_message(node_id: int):
    return struct.pack('ci', ELECTION_MESSAGE, node_id)
