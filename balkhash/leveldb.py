import json
import plyvel
import logging

from balkhash import settings
from balkhash.utils import to_bytes
from balkhash.dataset import Dataset, Bulk

log = logging.getLogger(__name__)


class LevelDBDataset(Dataset):

    def __init__(self, name, path=None):
        super(LevelDBDataset, self).__init__(name)
        path = path or settings.LEVELDB_PATH
        self.db = plyvel.DB(path, create_if_missing=True)
        self.client = self.db.prefixed_db(name.encode())

    def _make_key(self, entity_id, fragment):
        if entity_id is None:
            return None
        if fragment:
            entity_id = '.'.join((entity_id, fragment))
        return to_bytes(entity_id)

    def _serialize(self, entity):
        return json.dumps(entity).encode()

    def _deserialize(self, blob):
        # Python 3.5 and below don't accept binary input to json.loads
        return json.loads(blob.decode())

    def _encode(self, entity, fragment):
        entity = self._entity_dict(entity)
        key = self._make_key(entity.get('id'), fragment)
        entity = self._serialize(entity)
        return (key, entity)

    def delete(self, entity_id=None, fragment=None):
        prefix = self._make_key(entity_id, fragment)
        for key in self.client.iterator(prefix=prefix, include_value=False):
            self.client.delete(key)

    def put(self, entity, fragment=None):
        key, entity = self._encode(entity, fragment)
        return self.client.put(key, entity)

    def bulk(self, size=1000):
        return LevelDBBulk(self, size)

    def fragments(self, entity_id=None, fragment=None):
        prefix = self._make_key(entity_id, fragment)
        for key, blob in self.client.iterator(prefix=prefix):
            yield self._deserialize(blob)

    def close(self):
        self.db.close()


class LevelDBBulk(Bulk):

    def put(self, entity, fragment='default'):
        self.dataset.put(entity, fragment=fragment)

    def flush(self):
        # with self.dataset.client.write_batch() as batch:
        #     for (entity, fragment) in self.buffer:
        #         key, entity = self.dataset._encode(entity, fragment)
        #         batch.put(key, entity)
        pass
