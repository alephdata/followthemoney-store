import logging

from google.cloud import datastore

from .storage import Storage, StorageClient


log = logging.getLogger(__name__)


class GoogleDatastoreStorageClient(StorageClient):
    def __init__(self, namespace):
        self.client = datastore.Client(namespace=namespace)
        super().__init__(namespace)

    def _make_key(self, key, fragment_id):
        if fragment_id:
            return self.client.key('Entity', key, 'Entity', fragment_id)
        return self.client.key('Entity', key)

    def get(self, key, fragment_id=None):
        key = self._make_key(key, fragment_id)
        return self.client.get(key)

    def delete(self, key, fragment_id=None):
        key = self._make_key(key, fragment_id)
        return self.client.delete(key)

    def put(self, key, entity, fragment_id=None):
        assert isinstance(entity, dict)
        key = self._make_key(key, fragment_id)
        entity = datastore.Entity(key=key)
        entity.update(entity)
        return self.client.put(entity)

    def iterate(self, prefix=None):
        if prefix:
            ancestor = self.client.key('Entity', prefix)
            query = self.client.query(kind='Entity', ancestor=ancestor)
        else:
            query = self.client.query(kind='Entity')
        return query.fetch()


class GoogleDatastoreStorage(Storage):
    @property
    def client_class(self):
        return GoogleDatastoreStorageClient
