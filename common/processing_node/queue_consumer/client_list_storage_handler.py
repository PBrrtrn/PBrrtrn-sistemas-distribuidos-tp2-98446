from common.processing_node.queue_consumer.output_processor.storage_handler import StorageHandler


class ClientListStorageHandler(StorageHandler):

    def _generate_log_map(self, new_client_id):
        if "clients" not in self.storage:
            current_clients = []
        else:
            current_clients = self.storage["clients"]
        current_clients.append(new_client_id)
        return {"clients": current_clients}

    def _update_memory_map_with_logs(self, storage, log_map):
        storage["clients"] = log_map["clients"]

    def get_clients_list(self):
        return self.storage.get("clients", [])
