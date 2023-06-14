class Processor:
    def __init__(self):
        raise "Should not instantiate"

    def process(self, message_type, message):
        raise "Should implement"

    def process_eof(self, client_id):
        pass
