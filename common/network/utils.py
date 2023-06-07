from common.network.socket_wrapper import SocketWrapper


def receive_string(socket: SocketWrapper):
    str_len = receive_int(socket)
    return socket.recv(str_len).decode('utf-8')


def receive_int(socket: SocketWrapper):
    return int.from_bytes(socket.recv(4), 'big')
