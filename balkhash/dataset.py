class Dataset(object):
    def __init__(self, name, storage_client):
        self.name = name
        self.storage_client = storage_client

    def make_key(self, key, fragment_id):
        if fragment_id:
            return key + b'-v-' + fragment_id
        return key

    def get(self, key, fragment_id=None):
        key = self.make_key(key, fragment_id)
        return self.storage_client.get(key)

    def put(self, key, val, fragment_id=None):
        key = self.make_key(key, fragment_id)
        return self.storage_client.put(key, val)

    def iterate(self, prefix=None):
        return self.storage_client.iterate(prefix=prefix)

    def delete(self, key, fragment_id=None):
        key = self.make_key(key, fragment_id)
        return self.storage_client.delete(key)
