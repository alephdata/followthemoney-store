import logging
from google.cloud import datastore

from balkhash.dataset import Dataset

KIND = 'Entity'
log = logging.getLogger(__name__)


class GoogleDatastoreDataset(Dataset):

    def __init__(self, name):
        super(GoogleDatastoreDataset, self).__init__(name)
        self.client = datastore.Client(namespace=name)

    def _make_key(self, entity_id, fragment):
        if fragment:
            return self.client.key(KIND, entity_id, 'Fragment', fragment)
        if entity_id:
            return self.client.key(KIND, entity_id)

    def delete(self, entity_id=None, fragment=None):
        ancestor = self._make_key(entity_id, fragment)
        query = self.client.query(kind=KIND, ancestor=ancestor)
        for entity in query.fetch():
            self.client.delete(entity.key)

    def put(self, entity, fragment='default'):
        entity = self._entity_dict(entity)
        key = self._make_key(entity.get('id'), fragment)
        ent = datastore.Entity(key=key)
        ent.update(entity)
        return self.client.put(ent)

    def fragments(self, entity_id=None, fragment=None):
        ancestor = self._make_key(entity_id, fragment)
        query = self.client.query(kind=KIND, ancestor=ancestor)
        for entity in query.fetch():
            yield dict(entity.items())
