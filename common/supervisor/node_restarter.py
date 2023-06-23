from typing import Dict

import docker
import docker.errors


class NodeRestarter:
    def __init__(self, docker_client, node_id_to_container_name_mapping: Dict[int, str]):
        self.docker_client = docker_client
        self.node_id_to_container_name_mapping = node_id_to_container_name_mapping

    def restart_node(self, node_id: int):
        container_name = self.node_id_to_container_name_mapping[node_id]

        try:
            container = self.docker_client.containers.get(container_name)
            container.restart()
        except docker.errors.NotFound:
            print(f"ERROR - Container {container_name} not found")
