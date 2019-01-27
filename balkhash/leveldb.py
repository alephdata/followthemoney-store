import os
import json
import plyvel
import logging

from balkhash.utils import to_bytes
from balkhash.dataset import Dataset

log = logging.getLogger(__name__)


class LevelDBDataset(Dataset):
    PATH = os.getenv("BALKHASH_LEVELDB", "balkhashdb")

    def __init__(self, name):
        super(LevelDBDataset, self).__init__(name)
        db = plyvel.DB(self.PATH, create_if_missing=True)
        self.client = db.prefixed_db(name)

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

    def delete(self, entity_id=None, fragment=None):
        prefix = self._make_key(entity_id, fragment)
        for key, _ in self.client.iterator(prefix=prefix, include_value=False):
            self.client.delete(key)

    def put(self, entity, fragment=None):
        entity = self._entity_dict(entity)
        key = self._make_key(entity.get('id'), fragment)
        entity = self._serialize(entity)
        return self.client.put(key, entity)

    def fragments(self, entity_id=None, fragment=None):
        prefix = self._make_key(entity_id, fragment)
        for key, blob in self.client.iterator(prefix=prefix):
            yield self._deserialize(blob)
