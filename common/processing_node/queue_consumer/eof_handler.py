
class EOFHandler:
    def __init__(self):
        self.received_eof_signals = 0

    def __prepare(self, result, method, properties):
        return self.received_eof_signals + 1

    def __commit(self):
        pass

    def two_phase_commit(self, channel, result, method, properties):
        to_log = self.__prepare(result, method, properties)
        self._update_memory_map_with_logs(to_log)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.__commit()

    def _update_memory_map_with_logs(self, to_log):
        self.received_eof_signals = to_log

    def get_last_result(self):
        return None, None, None
