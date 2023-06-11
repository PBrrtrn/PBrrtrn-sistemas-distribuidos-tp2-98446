TYPE_HEADER_LEN = 1
ELECTION = b'0'
COORDINATOR = b'1'
ANSWER = b'2'


def election_message(node_id: int) -> bytes:
    return ELECTION + node_id.to_bytes(4, 'big')


def coordinator_message(node_id: int) -> bytes:
    return COORDINATOR + node_id.to_bytes(4, 'big')


def answer_message(node_id: int) -> bytes:
    return ANSWER + node_id.to_bytes(4, 'big')


def parse_message(message: bytes) -> (bytes, int):
    header = message[:TYPE_HEADER_LEN]
    body = message[TYPE_HEADER_LEN:]

    return header, int.from_bytes(body, 'big')