def identity_process_input(message_type: bytes, message_body: bytes):
    return message_type + message_body


def identity_process_input_without_header(_message_type: bytes, message_body: bytes):
    return message_body
