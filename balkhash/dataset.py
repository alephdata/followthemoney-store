class Dataset(object):
    def __init__(self, name, storage, namespace):
        self.name = name
        self.storage = storage
        self.namespace = namespace

    def make_key(self, key, fragment_id):
        if fragment_id:
            return key + b'-v-' + fragment_id
        return key

    def get(self, key, fragment_id=None):
        key = self.make_key(key, fragment_id)
        return self.storage.get(self.namespace, key)

    def put(self, key, val, fragment_id=None):
        key = self.make_key(key, fragment_id)
        return self.storage.put(self.namespace, key, val)

    def iterate(self, prefix=None):
        return self.storage.iterate(self.namespace, prefix=prefix)

    def delete(self, key, fragment_id=None):
        key = self.make_key(key, fragment_id)
        return self.storage.delete(self.namespace, key)
