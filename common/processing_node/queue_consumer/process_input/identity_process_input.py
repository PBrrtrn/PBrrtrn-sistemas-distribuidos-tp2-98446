def identity_process_input(message_type: bytes, message_body: bytes, client_id, message_id):
    return message_type + client_id.encode() + message_id.encode() + message_body


def identity_process_input_without_header(_message_type: bytes, message_body: bytes, _client_id):
    return message_body
