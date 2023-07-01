TYPE_HEADER_LEN = 1
ELECTION = b'0'
COORDINATOR = b'1'
ANSWER = b'2'
HEARTBEAT = b'3'
HEARTBEAT_ACK = b'4'
COORDINATOR_ACK = b'5'
COORDINATOR_DENY = b'6'


def election_message(node_id: int) -> bytes:
    return ELECTION + node_id.to_bytes(4, 'big')


def coordinator_message(node_id: int) -> bytes:
    return COORDINATOR + node_id.to_bytes(4, 'big')


def answer_message(node_id: int) -> bytes:
    return ANSWER + node_id.to_bytes(4, 'big')


def heartbeat_message(node_id: int) -> bytes:
    return HEARTBEAT + node_id.to_bytes(4, 'big')


def heartbeat_ack_message(node_id: int) -> bytes:
    return HEARTBEAT_ACK + node_id.to_bytes(4, 'big')


def coordinator_ack_message(node_id: int) -> bytes:
    return COORDINATOR_ACK + node_id.to_bytes(4, 'big')


def coordinator_deny_message(node_id: int) -> bytes:
    return COORDINATOR_DENY + node_id.to_bytes(4, 'big')


def parse_message(message: bytes) -> (bytes, int):
    header = message[:TYPE_HEADER_LEN]
    body = message[TYPE_HEADER_LEN:]

    return header, int.from_bytes(body, 'big')