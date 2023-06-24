
class StorageHandler:
    def __init__(self):
        pass

    def prepare(self, message: bytes):
        pass

    def commit(self):
        pass

    def update_changes_in_disk(self):
        pass

    def get_storage(self):
        pass

    def prepare_delete(self):
        pass

    def commit_delete(self):
        pass
