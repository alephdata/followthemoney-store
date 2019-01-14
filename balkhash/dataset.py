class Dataset(object):
    def __init__(self, name, storage_client):
        self.name = name
        self.storage_client = storage_client

    def get(self, key, fragment_id=None):
        return self.storage_client.get(key, fragment_id=fragment_id)

    def put(self, key, val, fragment_id=None):
        return self.storage_client.put(key, val, fragment_id=fragment_id)

    def iterate(self, prefix=None):
        return self.storage_client.iterate(prefix=prefix)

    def delete(self, key, fragment_id=None):
        return self.storage_client.delete(key, fragment_id=fragment_id)
