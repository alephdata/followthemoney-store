import logging
import os
import json

import plyvel

from balkhash.utils import to_bytes
from .storage import Storage, StorageClient


log = logging.getLogger(__name__)
DB_PATH = os.getenv("BALKHASH_LOCAL_DB", "balkhashdb")


class LevelDBStorageClient(StorageClient):
    def __init__(self, namespace):
        db = plyvel.DB(DB_PATH, create_if_missing=True)
        self.client = db.prefixed_db(namespace)
        super().__init__(namespace)

    def _serialize(self, entity):
        return json.dumps(entity).encode()

    def _deserialize(self, blob):
        # Python 3.5 and below don't accept binary input to json.loads
        return json.loads(blob.decode())

    def _make_key(self, key, fragment_id):
        if fragment_id:
            key = key + '-v-' + fragment_id
        return to_bytes(key)

    def get(self, key, fragment_id=None):
        key = self._make_key(key, fragment_id)
        blob = self.client.get(key)
        if blob:
            return self._deserialize(blob)

    def delete(self, key, fragment_id=None):
        key = self._make_key(key, fragment_id)
        return self.client.delete(key)

    def put(self, key, entity, fragment_id=None):
        key = self._make_key(key, fragment_id)
        entity = self._serialize(entity)
        return self.client.put(key, entity)

    def iterate(self, prefix=None):
        if prefix:
            prefix = to_bytes(prefix)
        for key, blob in self.client.iterator(prefix=prefix):
            yield key.decode(), self._deserialize(blob)


class LevelDBStorage(Storage):
    CLIENT_CLASS = LevelDBStorageClient
