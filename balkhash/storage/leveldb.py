import logging
import os

import plyvel

from .storage import Storage, StorageClient


log = logging.getLogger(__name__)
DB_PATH = os.getenv("BALKHASH_LOCAL_DB", "balkhashdb")


class LevelDBStorageClient(StorageClient):
    def __init__(self, namespace):
        db = plyvel.DB(DB_PATH, create_if_missing=True)
        self.client = db.prefixed_db(namespace)
        super().__init__(namespace)

    def get(self, key):
        return self.client.get(key)

    def delete(self, key):
        return self.client.delete(key)

    def put(self, key, val):
        return self.client.put(key, val)

    def iterate(self, prefix=None):
        return self.client.iterator(prefix=prefix)


class LevelDBStorage(Storage):
    CLIENT_CLASS = LevelDBStorageClient
