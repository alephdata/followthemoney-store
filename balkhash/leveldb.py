import json
import plyvel
import logging

from balkhash import settings
from balkhash.utils import to_bytes
from balkhash.dataset import Dataset, Bulk, DEFAULT

log = logging.getLogger(__name__)


class LevelDBDataset(Dataset):

    def __init__(self, config):
        super(LevelDBDataset, self).__init__(config)
        path = config.get('path', settings.LEVELDB_PATH)
        self.db = plyvel.DB(path, create_if_missing=True)
        self.client = self.db.prefixed_db(self.name.encode())

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

    def put(self, entity, fragment=DEFAULT):
        key, entity = self._encode(entity, fragment or DEFAULT)
        return self.client.put(key, entity)

    def bulk(self, size=1000):
        return LevelDBBulk(self, size)

    def close(self):
        self.db.close()

    def fragments(self, entity_id=None, fragment=None):
        prefix = self._make_key(entity_id, fragment)
        for key, blob in self.client.iterator(prefix=prefix):
            yield self._deserialize(blob)

    def __len__(self):
        count = 0
        prev = None
        for key in self.client.iterator(include_value=False):
            key = key.split(b'.', 1)[0]
            if prev != key:
                count += 1
                prev = key
        return count

    def __repr__(self):
        return '<LevelDBDataset(%r)>' % self.client


class LevelDBBulk(Bulk):

    def put(self, entity, fragment=DEFAULT):
        self.dataset.put(entity, fragment=fragment)

    def flush(self):
        # with self.dataset.client.write_batch() as batch:
        #     for ((_, fragment), entity) in self.buffer.items():
        #         key, entity = self.dataset._encode(entity, fragment)
        #         batch.put(key, entity)
        pass
