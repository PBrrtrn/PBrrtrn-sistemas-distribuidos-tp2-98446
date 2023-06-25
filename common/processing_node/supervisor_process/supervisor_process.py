import multiprocessing


class SupervisorProcess:
    ELECTION_TIMEOUT = 0.5

    def __init__(self, node_id: int, network_size: int):
        # TODO: Recibir ID por parámetro
        self.node_id = node_id
        self.network_size = network_size
        self.process = multiprocessing.Process(target=self._supervisor)

    def _supervisor(self):
        self._run_election()

    def _run_election(self):
        pass
