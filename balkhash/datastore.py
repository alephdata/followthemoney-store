import json
import logging
from google.cloud import datastore

from balkhash.dataset import Dataset, Bulk

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

    def _encode(self, entity, fragment):
        entity = self._entity_dict(entity)
        key = self._make_key(entity.get('id'), fragment)
        exclude = ('properties', 'context')
        ent = datastore.Entity(key=key, exclude_from_indexes=exclude)
        ent['id'] = entity.pop('id')
        ent['schema'] = entity.pop('schema')
        ent['properties'] = json.dumps(entity.pop('properties', {}))
        ent['context'] = json.dumps(entity)
        return ent

    def delete(self, entity_id=None, fragment=None):
        ancestor = self._make_key(entity_id, fragment)
        query = self.client.query(kind='Fragment', ancestor=ancestor)
        batch = []
        for entity in query.fetch():
            batch.append(entity.key)
            if len(batch) >= 500:
                self.client.delete_multi(batch)
                batch = []
        if len(batch):
            self.client.delete_multi(batch)

    def put(self, entity, fragment='default'):
        entity = self._encode(entity, fragment)
        return self.client.put(entity)

    def bulk(self, size=500):
        return GoogleDatastoreBulk(self, size)

    def fragments(self, entity_id=None, fragment=None):
        ancestor = self._make_key(entity_id, fragment)
        query = self.client.query(kind='Fragment', ancestor=ancestor)
        for entity in query.fetch():
            data = json.loads(entity['context'])
            data['id'] = entity['id']
            data['schema'] = entity['schema']
            data['properties'] = json.loads(entity['properties'])
            yield data


class GoogleDatastoreBulk(Bulk):

    def flush(self):
        entities = [self.dataset._encode(e, f) for (e, f) in self.buffer]
        if not len(entities):
            return
        if len(entities) == 1:
            self.dataset.client.put(entities[0])
        self.dataset.client.put_multi(entities)
