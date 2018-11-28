class Dataset(object):
    def __init__(self, name, storage, bucket):
        self.name = name
        self.storage = storage
        self.bucket = bucket
        pass

    def exists(self, key, context):
        key = context + "/" + key
        return self.storage.exists(self.bucket, key)

    def get(self, key, context):
        key = context + "/" + key
        return self.storage.get(self.bucket, key)

    def put(self, key, val, context):
        key = context + "/" + key
        return self.storage.put(self.bucket, key, val)

    def list_blobs(self, context):
        prefix = context + "/"
        return self.storage.list_keys(self.bucket, prefix)

    def get_blob_url(self, key, context):
        key = context + "/" + key
        return self.storage.get_blob_url(self.bucket, key)
