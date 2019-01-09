import logging
import json

from google.cloud import datastore

from .storage import Storage, StorageClient


log = logging.getLogger(__name__)


class GoogleDatastoreStorageClient(StorageClient):
    def __init__(self, namespace):
        self.client = datastore.Client(namespace=namespace)
        super().__init__(namespace)

    def get(self, key):
        key = self.client.key('Entity', key)
        return self.client.get(key)

    def delete(self, key):
        key = self.client.key('Entity', key)
        return self.client.delete(key)

    def put(self, key, val):
        key = self.client.key('Entity', key)
        entity = datastore.Entity(key=key)
        entity.update(json.loads(val))
        return self.client.put(entity)

    def iterate(self, prefix=None):
        query = self.client.query(kind='Entity')
        return query.fetch()


class GoogleDatastoreStorage(Storage):
    CLIENT_CLASS = GoogleDatastoreStorageClient
